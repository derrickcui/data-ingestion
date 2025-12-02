# app/sources/base64_source.py
from typing import Dict, Any, Optional
from app.sources.base import BaseSource
import base64


class Base64Source(BaseSource):
    def __init__(self, filename: str, base64_str: str):
        self.filename = filename
        self.base64_str = base64_str

    def read(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if context is None:
            context = {}

        context.setdefault("file", {})["filename"] = self.filename

        try:
            content = base64.b64decode(self.base64_str)
        except Exception as e:
            raise RuntimeError(f"Invalid base64 content: {e}")

        return {
            "file_name": self.filename,
            "binary": content
        }
