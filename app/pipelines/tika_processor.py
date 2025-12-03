import os
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import requests
from app.pipelines.base import BaseProcessor
from app.utility.log import logger
from app.utility.config import Config


class TikaProcessor(BaseProcessor):
    """终极生产级 Tika 解析器（2025 大厂标配版）。ID 生成逻辑已移至 IdProcessor。"""
    order = 10
    TIKA_SERVER = Config.TIKA_SERVICE_URL
    TIMEOUT = Config.TIKA_SERVICE_TIMEOUT

    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        binary = data.get("binary")
        stable_doc_id = data.get("doc_id")
        file_name = data.get("file_name", "unknown_file")
        file_ext = os.path.splitext(file_name)[1].lstrip('.').lower() or "unknown"

        # 获取上游（Source）传入的用户自定义元数据
        user_metadata = data.get("user_metadata", {})

        # NEW: 提取用户指定的 ingestion_method
        # 优先级：user_metadata > data.ingestion_method > data.type (Source 提供的) > 默认值
        ingestion_method = (
            user_metadata.get("ingestion_method")
            or data.get("ingestion_method")
            or data.get("source_type")  # <-- 修复点：检查 Source 提供的 'type' 字段
            or "file_upload"  # 默认值
        )

        # ============================ 特殊处理 Web 文本 ============================
        if data.get("source_type") == "web" and "raw_text" in data and data["raw_text"]:
            logger.info(f"TikaProcessor: using pre-fetched raw_text for web source {file_name}")
            raw_text = data["raw_text"]
            merged_metadata = {
                "doc_id": stable_doc_id,
                "ingestion_method": ingestion_method,
                **user_metadata
            }
            merged_metadata["source_name"] = file_name
            merged_metadata["source_type"] = file_ext
            merged_metadata["raw_text_length"] = len(raw_text)
            return {"raw_text": raw_text, "metadata": merged_metadata}

        # ============================ 无 binary 情况 ============================
        if not binary:
            logger.warning("TikaProcessor: no binary data. Returning raw_text and merging user_metadata.")
            # 修复点: 即使没有 binary，也必须确保返回的 metadata 中包含完整的 user_metadata
            merged_metadata = {
                "doc_id": stable_doc_id,
                "ingestion_method": ingestion_method,
                **user_metadata  # <-- 核心修复：合并所有用户传入的元数据
            }
            return {"raw_text": data.get("raw_text"), "metadata": merged_metadata}

        if not stable_doc_id:
            logger.error("TikaProcessor failed: doc_id not found in data. IdProcessor may have been skipped.")
            # 强行设置一个 fallback ID 以避免崩溃，但这是错误情况
            stable_doc_id = "error_fallback_" + hashlib.sha256(binary).hexdigest()[:10]

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
                doc_id=stable_doc_id,  # 使用 IdProcessor 传入的 ID
                user_metadata=user_metadata,
                ingestion_method=ingestion_method,  # NEW: 传入上传方式
            )

            logger.info(
                f"TikaProcessor 成功 | doc_id: {stable_doc_id} | "
                f"文件: {file_name} | 长度: {len(raw_text)} | "
                f"页数: {metadata.get('page_count', 'N/A')} | "
                f"标题: {metadata.get('title', '无标题')[:60]} | "
                f"上传方式: {ingestion_method}"  # NEW: Log中显示上传方式
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
            user_metadata: Dict[str, Any],
            ingestion_method: str,  # NEW: 接收上传方式
    ) -> Dict[str, Any]:

        # Helper: 优先获取存在的 key
        def get(*keys, default=""):
            for k in keys:
                v = raw_meta.get(k)
                if v is not None:
                    # 确保处理列表类型的值
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
        m["doc_id"] = doc_id  # 直接使用传入的 ID
        m["ingestion_method"] = ingestion_method  # NEW: 记录上传方式
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

        # 页数安全处理
        pages = get("xmpTPg:NPages", "pdf:NPages", "Page-Count", "NumberOfPages")
        try:
            m["page_count"] = int(pages) if pages and str(pages).isdigit() else 0
        except:
            m["page_count"] = 0

        # 业务字段
        kw = get("keywords", "meta:keyword", "binary:Keywords") or ""
        m["keywords"] = [k.strip() for k in str(kw).split(",") if k.strip()]
        m["company"] = get("Company", "dc:publisher") or ""
        m["category"] = get("Category") or ""  # 密级常在这里
        m["producer"] = raw_meta.get("pdf:Producer", "") or ""

        # 关键状态
        m["is_encrypted"] = raw_meta.get("pdf:encrypted") == "true"

        # ==================== 检测扫描 PDF ====================
        if file_ext in ["pdf"]:
            m["is_scanned_pdf"] = self._detect_scanned_pdf(raw_text, m)
        else:
            m["is_scanned_pdf"] = False  # 非 PDF 文件直接标记 False

        m["raw_text_length"] = len(raw_text)

        # ==================== NEW: 合并用户自定义元数据 ====================
        m.update(user_metadata)
        if "ingestion_method" not in m:
            m["ingestion_method"] = ingestion_method
        logger.debug(f"Merged user metadata: {user_metadata.keys()}")

        return m

    # ============================== 扫描 PDF 检测 ==============================
    @staticmethod
    def _detect_scanned_pdf(text: str, meta: dict) -> bool:
        producer = (meta.get("producer") or "").lower()
        scan_keywords = [
            "scan", "image", "mfp", "scanner", "canon", "fujitsu",
            "kodak", "hp", "ricoh", "epson", "pdfscan"
        ]
        if any(k in producer for k in scan_keywords):
            return True

        page_count = meta.get("page_count") or 0
        if not isinstance(page_count, int):
            try:
                page_count = int(page_count)
            except:
                page_count = 0

        if len(text.strip()) < 600 and page_count > 3:
            return True

        return False
