import os
import re
import hashlib
from typing import Dict, Any, Optional, Union
from app.pipelines.base import BaseProcessor  # 假设 BaseProcessor 位于 app.pipelines.base
from app.utility.log import logger


# -------------------------------------------------
# 辅助函数：文件名清理
# -------------------------------------------------
def clean_filename_keep_chinese(text: str) -> str:
    """
    彻底清除文件名里的垃圾符号，只保留中文、英文、数字、下划线、点、短横线。
    与原代码保持一致。
    """
    garbage = '!"#$%&\'()*+,-/:;<=>?@[\\]^_`{|}~“”‘’《》〈〉‹›«»„“‟′″‵′〃＂[]【】'
    text = text.translate(str.maketrans('', '', garbage))
    # 允许中文、英文、数字、下划线、点、短横线
    return re.sub(r'[^\u4e00-\u9fff\w\.\-]+', '', text)


# -------------------------------------------------
# 核心函数：稳定 Doc ID 生成器
# -------------------------------------------------
def generate_stable_doc_id(
        content_for_hash: Union[bytes, str],
        file_name: str,
        preferred_doc_id: Optional[str] = None,
        source_system: str | None = None,
        include_filename: bool = True,
) -> str:
    """
    企业级最强 doc_id 生成器。

    优先级：
    1. 业务系统主动传入的 ID
    2. 清洗后的文件名 + 文件内容哈希（推荐！既去重又保留版本）

    Args:
        content_for_hash: 用于哈希的内容。对于 file/base64 是 bytes；对于 text/uri 是 str。
        file_name: 原始文件名或标识名。
        preferred_doc_id: 业务系统提供的预设 ID。
        source_system: 来源系统标识（默认 'rag_upload'）。
        include_filename: 是否将文件名包含在哈希中（用于版本控制）。
    """
    if preferred_doc_id and preferred_doc_id.strip():
        return preferred_doc_id.strip()

    # 统一将内容转换为 bytes 进行哈希
    if isinstance(content_for_hash, str):
        content_bytes = content_for_hash.encode("utf-8")
    elif isinstance(content_for_hash, bytes):
        content_bytes = content_for_hash
    else:
        # Fallback for unexpected type
        content_bytes = str(content_for_hash).encode("utf-8")

    hasher = hashlib.sha256()
    if include_filename:
        clean_name = clean_filename_keep_chinese(file_name)
        hasher.update(clean_name.encode("utf-8"))
        hasher.update(b"\0\0")  # 分隔符，防止哈希碰撞

    # 使用统一的 bytes 内容进行哈希
    hasher.update(content_bytes)

    return f"{source_system}_{hasher.hexdigest()[:16]}"


# -------------------------------------------------
# IdProcessor 类
# -------------------------------------------------
class IdProcessor(BaseProcessor):
    """
    IdProcessor 负责为所有类型的输入源（file, text, uri, base64）生成稳定且唯一的 doc_id。
    它应该在所有其他内容处理器之前执行。
    """
    order = 5  # 确保在 TikaProcessor (order=10) 之前运行

    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        从 data 中提取必要信息，生成 doc_id，并将其注入到 data 和 metadata 中。
        """

        # 1. 确定用于哈希的内容
        # - file/base64 Source 传入 binary (bytes)
        # - text Source 传入 raw_text (str)
        # - uri Source 传入 uri (str)
        content_for_hash = data.get("binary") or data.get("raw_text") or data.get("uri")
        file_name = data.get("file_name", "unknown_source")

        if not content_for_hash:
            logger.error("IdProcessor failed: No content for hash calculation (binary, raw_text, or uri)")
            # 即使失败也尝试继续，可能上游有 preferred_doc_id
            content_for_hash = "no_content"

        # 2. 获取优先 ID (Preferred ID)
        user_metadata = data.get("user_metadata", {})
        logger.info(f">>>>user_metadata={user_metadata}")
        preferred_id = (
                user_metadata.get("doc_id")  # 1. API 传入的 user_metadata 中的 doc_id
                or data.get("doc_id")  # 2. 上游 Source 可能设置的 doc_id
                or data.get("business_id")
                or data.get("archive_no")
                or data.get("id")
        )

        # 3. 生成 ID
        api_source_system = user_metadata.get("source_system")
        logger.info(f"api_source_system:{api_source_system}")
        stable_doc_id = generate_stable_doc_id(
            content_for_hash=content_for_hash,
            file_name=file_name,
            preferred_doc_id=preferred_id,
            source_system=api_source_system,
            include_filename=True,
        )

        # 4. 将 doc_id 注入到 data 和 metadata 中
        data["doc_id"] = stable_doc_id

        # 确保 metadata 字典存在
        if "metadata" not in data or data["metadata"] is None:
            data["metadata"] = {}

        data["metadata"]["doc_id"] = stable_doc_id

        logger.info(f"IdProcessor: Generated doc_id: {stable_doc_id} for source '{file_name}'")

        # 返回结果，注意这里返回的是 data 本身，因为 BaseProcessor 要求返回 dict
        # 我们在这里直接修改了 data，并返回它，以便流水线继续
        return data