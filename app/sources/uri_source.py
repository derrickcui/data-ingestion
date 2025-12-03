# app/sources/uri_source.py
from typing import Dict, Any, Optional
from app.sources.base import BaseSource
import requests


class URISource(BaseSource):
    def __init__(self, uri: str, filename: Optional[str] = None):
        self.uri = uri
        self.filename = filename or uri.split("/")[-1] or "remote_file"

    def read(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if context is None:
            context = {}

        context.setdefault("file", {})["filename"] = self.filename

        # 下载 URI 内容
        resp = requests.get(self.uri, timeout=20)
        resp.raise_for_status()

        return {
            "file_name": self.filename,
            "binary": resp.content,
            "source_type": "text"
        }
