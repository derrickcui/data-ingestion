import os
import re
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import requests
from app.pipelines.base import BaseProcessor
from app.utility.log import logger
from app.utility.config import Config


def clean_filename_keep_chinese(text: str) -> str:
    """彻底清除文件名里的垃圾符号，只保留中文、英文、数字、下划线、点、短横线"""
    garbage = '!"#$%&\'()*+,-/:;<=>?@[\\]^_`{|}~“”‘’《》〈〉‹›«»„“‟′″‵′〃＂[]【】'
    text = text.translate(str.maketrans('', '', garbage))
    return re.sub(r'[^\u4e00-\u9fff\w\.\-]+', '', text)


def generate_stable_doc_id(
        binary: bytes,
        file_name: str,
        preferred_doc_id: Optional[str] = None,
        source_system: str | None = None,
        include_filename: bool = True,
) -> str:
    """
    企业级最强 doc_id 生成器
    优先级：
    1. 业务系统主动传入的 ID
    2. 清洗后的文件名 + 文件内容哈希（推荐！既去重又保留版本）
    """
    if preferred_doc_id and preferred_doc_id.strip():
        return preferred_doc_id.strip()

    source_system = source_system or os.getenv("SOURCE_SYSTEM", "rag_upload")

    hasher = hashlib.sha256()
    if include_filename:
        clean_name = clean_filename_keep_chinese(file_name)
        hasher.update(clean_name.encode("utf-8"))
        hasher.update(b"\0\0")  # 分隔符，防止哈希碰撞
    hasher.update(binary)

    return f"{source_system}_{hasher.hexdigest()[:16]}"


class TikaProcessor(BaseProcessor):
    """终极生产级 Tika 解析器（2025 大厂标配版）"""
    order = 10
    TIKA_SERVER = Config.TIKA_SERVICE_URL
    TIMEOUT = Config.TIKA_SERVICE_TIMEOUT

    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        binary = data.get("binary")
        file_name = data.get("file_name", "unknown_file")
        file_ext = os.path.splitext(file_name)[1].lstrip('.').lower() or "unknown"

        # 获取上游（FileSource）传入的用户自定义元数据
        # 假设 FileSource/PipelineRunner 将用户元数据放在 "user_metadata" 键下
        user_metadata = data.get("user_metadata", {})

        logger.info(f"》》》》user_metadata:{user_metadata}")
        if not binary:
            logger.warning("TikaProcessor: no binary data")
            return {"raw_text": "", "metadata": {}}

        # ==================== 生成企业级稳定 doc_id ====================
        # 在生成 doc_id 时，优先使用用户元数据或传入的业务 ID
        preferred_id = (
                user_metadata.get("doc_id")  # 优先使用用户在 API 中传入的 doc_id
                or data.get("doc_id")
                or data.get("business_id")
                or data.get("archive_no")
                or data.get("id")
        )
        stable_doc_id = generate_stable_doc_id(
            binary=binary,
            file_name=file_name,
            preferred_doc_id=preferred_id,
            source_system=os.getenv("SOURCE_SYSTEM", "rag_upload"),
            include_filename=True,  # 必须开！保留版本信息
        )

        try:
            # ==================== 提取纯文本 ====================
            tika_resp = requests.put(
                f"{self.TIKA_SERVER}/tika",
                data=binary,
                headers={
                    "Accept": "text/plain",
                    "File-Name": file_name.encode("utf-8"),
                },
                timeout=self.TIMEOUT,
            )
            tika_resp.raise_for_status()
            if tika_resp.encoding is None or tika_resp.encoding.lower() != "utf-8":
                tika_resp.encoding = "utf-8"
            raw_text = tika_resp.text

            # ==================== 提取完整 Metadata ====================
            meta_resp = requests.put(
                f"{self.TIKA_SERVER}/meta",
                data=binary,
                headers={
                    "Accept": "application/json",
                    "File-Name": file_name.encode("utf-8"),
                },
                timeout=self.TIMEOUT,
            )
            meta_resp.raise_for_status()
            raw_meta = meta_resp.json()

            # ==================== 终极归一化 + 增强 ====================
            metadata = self._normalize_and_enhance_metadata(
                raw_meta=raw_meta,
                file_name=file_name,
                file_ext=file_ext,
                raw_text=raw_text,
                binary=binary,
                doc_id=stable_doc_id,
                user_metadata=user_metadata,  # <-- NEW: 传入用户自定义元数据
            )

            logger.info(
                f"TikaProcessor 成功 | doc_id: {stable_doc_id} | "
                f"文件: {file_name} | 长度: {len(raw_text)} | "
                f"页数: {metadata.get('page_count', 'N/A')} | "
                f"标题: {metadata.get('title', '无标题')[:60]}"
            )

            return {
                "raw_text": raw_text,
                "metadata": metadata,
            }

        except requests.RequestException as e:
            logger.error(f"Tika Server 错误 ({file_name}): {e}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"状态码: {e.response.status_code} | 内容: {e.response.text[:500]}")
            raise
        except Exception as e:
            logger.error(f"TikaProcessor 未知错误 ({file_name}): {e}", exc_info=True)
            raise

    # ============================== 终极元数据处理 ==============================
    def _normalize_and_enhance_metadata(
            self,
            raw_meta: dict,
            file_name: str,
            file_ext: str,
            raw_text: str,
            binary: bytes,
            doc_id: str,
            user_metadata: Dict[str, Any],  # <-- NEW: 接收用户元数据
    ) -> Dict[str, Any]:

        def get(*keys, default=""):
            for k in keys:
                v = raw_meta.get(k)
                if v is not None:
                    return v[0] if isinstance(v, list) else v
            return default

        def parse_date(val):
            if not val:
                return None
            s = str(val).replace("Z", "+00:00").split("+")[0].split(".")[0]
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).isoformat()
                except:
                    continue
            return str(val)

        m = {}

        # 核心身份
        m["doc_id"] = doc_id
        m["source_name"] = file_name
        m["source_type"] = file_ext
        m["source_size"] = len(binary)
        m["content_md5"] = hashlib.md5(binary).hexdigest()
        m["content_sha256"] = hashlib.sha256(binary).hexdigest()
        m["ingest_at"] = datetime.now(timezone.utc).isoformat(sep="T", timespec="milliseconds")

        # 文档属性（title 保持原始美观，不过度清洗）
        m["title"] = get("dc:title", "title", "pdf:docinfo:title", "subject") or os.path.splitext(file_name)[0]
        m["author"] = get("dc:creator", "meta:author", "creator", "Author", "pdf:Author", "pdf:docinfo:creator") or ""
        m["created_at"] = parse_date(get("dcterms:created", "meta:creation-date", "Creation-Date", "date"))
        m["modified_at"] = parse_date(get("dcterms:modified", "Last-Modified", "meta:save-date"))
        m["language"] = get("language", "dc:language", "Content-Language") or "zh-CN"

        # 页数
        pages = get("xmpTPg:NPages", "pdf:NPages", "Page-Count", "NumberOfPages")
        m["page_count"] = int(pages) if pages and str(pages).isdigit() else None

        # 业务字段
        kw = get("keywords", "meta:keyword", "binary:Keywords") or ""
        m["keywords"] = [k.strip() for k in str(kw).split(",") if k.strip()]
        m["company"] = get("Company", "dc:publisher") or ""
        m["category"] = get("Category") or ""  # 密级常在这里
        m["producer"] = raw_meta.get("pdf:Producer", "") or ""

        # 关键状态（后面清洗/OCR 必须依赖）
        m["is_encrypted"] = raw_meta.get("pdf:encrypted") == "true"
        m["is_scanned_pdf"] = self._detect_scanned_pdf(raw_text, m)
        m["raw_text_length"] = len(raw_text)

        # ==================== NEW: 合并用户自定义元数据 ====================
        # 用户提供的元数据具有最高优先级，覆盖Tika解析的值
        m.update(user_metadata)
        logger.debug(f"Merged user metadata: {user_metadata.keys()}")
        # ===================================================================

        return m

    @staticmethod
    def _detect_scanned_pdf(text: str, meta: dict) -> bool:
        producer = meta.get("producer", "").lower()
        scan_keywords = ["scan", "image", "mfp", "scanner", "canon", "fujitsu", "kodak", "hp", "ricoh", "epson",
                         "pdfscan"]
        if any(k in producer for k in scan_keywords):
            return True
        if len(text.strip()) < 600 and meta.get("page_count", 0) > 3:
            return True
        return False