from typing import Dict, Any
import uuid
import datetime


class AssembleProcessor:
    """
    把所有 processors 的中间结果整合成统一 JSON Document。
    这是整个 pipeline 的最终产物（用于 Solr / Chroma / 前端展示）。
    """

    def process(self, data: Dict[str, Any], context: Dict[str, Any] = None) -> Dict[str, Any]:
        context = context or {}

        # 从之前 processors 获取内容
        raw_text = data.get("raw_text", "")
        clean_text = data.get("clean_text", "")
        chunks = data.get("chunks", [])
        embeddings = data.get("embeddings", [])
        llm_metadata = data.get("llm_metadata", {})

        # 生成统一 doc_id（如 context 中已有则沿用）
        doc_id = context.get("doc_id") or str(uuid.uuid4())

        # 统一 metadata
        document = {
            "doc_id": doc_id,
            "source_path": context.get("source_path"),
            "timestamp": datetime.datetime.utcnow().isoformat(),

            # --- 展示用 ---
            "raw_text": raw_text,            # 原文（用于展示）
            "clean_text": clean_text,        # 清洗后文本（用于全文检索）

            # --- Chunk + Embedding ---
            "chunks": chunks,                # 分片文本
            "embeddings": embeddings,        # 每个 chunk 对应 embedding

            # --- LLM 增强 ---
            "metadata": llm_metadata         # business glossary、topics 等
        }

        return document
