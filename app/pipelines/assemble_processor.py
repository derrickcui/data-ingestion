from typing import Dict, Any, Optional
import uuid
import datetime
from app.pipelines.base import BaseProcessor

class AssembleProcessor(BaseProcessor):
    """
    把所有 processors 的中间结果整合成统一 JSON Document。
    这是整个 pipeline 的最终产物（用于 Solr / Chroma / 前端展示）。
    """

    order = 100  # 通常放在最后执行

    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        context = context or {}

        # 从之前 processors 获取内容
        raw_text = data.get("raw_text", "")
        clean_text = data.get("clean_text", "")
        chunks = data.get("chunks", [])
        embeddings = data.get("embeddings", [])
        llm_metadata = data.get("metadata", {})  # 与之前 LLMProcessor 返回的 key 对齐

        # 生成统一 doc_id（如 context 中已有则沿用）
        doc_id = context.get("doc_id") or str(uuid.uuid4())

        # 统一 metadata
        document = {
            "doc_id": doc_id,
            "source_path": context.get("source_path"),
            "timestamp": datetime.datetime.utcnow().isoformat(),

            # --- 展示用 ---
            "raw_text": raw_text,
            "clean_text": clean_text,

            # --- Chunk + Embedding ---
            "chunks": chunks,
            "embeddings": embeddings,

            # --- LLM 增强 ---
            "metadata": llm_metadata
        }

        return document
