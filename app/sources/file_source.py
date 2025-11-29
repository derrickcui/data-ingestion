# app/sources/file_source.py
from typing import Dict, Any, Optional
from app.sources.base import BaseSource


class FileSource(BaseSource):
    def __init__(self, filename: str, content: bytes):
        self.filename = filename
        self.content = content

    def read(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        # 可把文件元信息注入 context 中（可选）
        if context is None:
            context = {}
        context.setdefault("file", {})["filename"] = self.filename

        return {
            "filename": self.filename,
            "binary": self.content
        }
