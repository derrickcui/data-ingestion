# app/sources/text_source.py
from typing import Dict, Any, Optional
from app.sources.base import BaseSource


class TextSource(BaseSource):
    def __init__(self, text: str, filename: str = "text_input.txt"):
        self.user_metadata = None
        self.text = text
        self.filename = filename

    def read(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if context is None:
            context = {}

        context.setdefault("file", {})["filename"] = self.filename

        result = {
            "file_name": self.filename,
            "raw_text": self.text,        # ← 关键！必须传 raw_text
            "source_type": "text"
        }

        # ✅ 如果 user_metadata 存在，就放进返回的 dict
        if self.user_metadata:
            result["user_metadata"] = self.user_metadata

        return result
