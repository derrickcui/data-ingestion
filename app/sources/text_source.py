# app/sources/text_source.py
from typing import Dict, Any, Optional
from app.sources.base import BaseSource


class TextSource(BaseSource):
    def __init__(self, text: str, filename: str = "text_input.txt"):
        self.text = text
        self.filename = filename

    def read(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if context is None:
            context = {}

        context.setdefault("file", {})["filename"] = self.filename

        return {
            "file_name": self.filename,
            "binary": self.text.encode("utf-8")
        }
