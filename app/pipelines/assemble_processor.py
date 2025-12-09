# app/processors/assemble.py
import json
from typing import Dict, Any, Optional
import uuid
from datetime import datetime, timezone
from app.pipelines.base import BaseProcessor
from app.utility.log import logger
from app.utility.utils import generate_professional_uuid_id


class AssembleProcessor(BaseProcessor):
    order = 100

    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        context = context or {}
        if "binary" in data:
            del data["binary"]
        raw_text = data.get("raw_text", "")
        clean_text = data.get("clean_text", "")
        chunks = data.get("chunks", [])
        embeddings = data.get("embeddings", [])
        metadata = data.get("metadata", {}) or {}
        doc_id = metadata.get("doc_id") or str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat(sep="T", timespec="milliseconds")

        # ============ 1. 主文档（仅用于 Solr 等混合检索库） ============
        main_doc = {
            "id": generate_professional_uuid_id(doc_id),
            "doc_id": doc_id,
            "doc_type": "document",
            "raw_content": raw_text,
            "content": clean_text,
            "title": metadata.get("title", ""),
            "author": metadata.get("author", ""),
            "source_name": metadata.get("source_name", data.get("file_name", "")),
            "source_type": metadata.get("source_type", ""),
            "source_path": data.get("source_path", ""),

            "source": metadata.get("source", ""),
            "created_at": metadata.get("created_at", ""),
            "modified_at": metadata.get("modified_at", ""),
            "keywords": metadata.get("keywords", ""),
            "summary": metadata.get("summary", ""),
            "section_title": metadata.get("section_title", ""),
            "language": metadata.get("language", ""),
            "chunk_count": len(embeddings or []),
            "timestamp": now,
            **{k: v for k, v in metadata.items() if k not in {"title", "author", "filename", "filetype"}},
        }

        # ============ 2. Chunk 文档（Solr + 所有向量库都需要） ============
        chunk_docs = [
            {
                "id": generate_professional_uuid_id(f"{doc_id}_chunk_{idx:06d}"),
                "doc_id": f"{doc_id}_chunk_{idx:06d}",
                "doc_type": "chunk",
                "parent_id": main_doc["id"],
                "chunk_index": idx,
                "chunk_content": chunk_text,
                "_gl_vector": embedding.get('embedding',[]),  # Solr 用的向量字段

                # 继承主文档元数据，方便过滤
                "title": main_doc["title"],
                "author": main_doc["author"],
                "source_name": main_doc["source_name"],
                "source_type": main_doc["source_type"],
                "source_path": main_doc["source_path"],
                "timestamp": now,

                # 额外字段供向量库使用（可自由扩展）
                #"metadata": {                          # 向量库 metadata
                #    "doc_id": parent_id,
                #    "chunk_index": idx,
                #    "source_path": main_doc["source_path"],
                #    "title": main_doc["title"],
                #}
            }
            for idx, (chunk_text, embedding) in enumerate(zip(chunks, embeddings))
        ]

        # ============ 最终返回：一次返回两种结构，所有 Sink 各取所需 ============
        return {
            "solr_docs": [main_doc, *chunk_docs],    # Solr 直接吃这个
            "vector_docs": chunk_docs,               # 所有向量库只吃这个
            "doc_id": doc_id,                        # 方便日志追踪
        }