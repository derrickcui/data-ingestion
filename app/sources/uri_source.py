# app/sources/uri_source.py
import os
import re

import requests
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse
from app.sources.base import BaseSource
from app.utility.log import logger


class URISource(BaseSource):
    """
    可同时处理：
    - Windows / Linux / macOS 本地文件或 file:/// URI
    - HTTP / HTTPS 下载
    - 本地文件夹递归扫描
    """

    def __init__(self, uri: str):
        self.uri = uri.strip('"').strip("'")  # 去掉 Swagger 带的引号
        self.user_metadata = None
        self.source_type = "uri"

    def read(self, context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        返回统一列表，每个文件都是 dict，兼容 FileSource:
        [
            {
                "file_name": ...,
                "binary": ...,
                "source_type": "uri",
                "user_metadata": {...}
            },
            ...
        ]
        """
        if context is None:
            context = {}

        uri = self.uri
        logger.info(f"[URISource] Input URI = {uri}")

        files_data = []

        # 1. file:/// URI
        if uri.startswith("file:///"):
            path = uri.replace("file:///", "")
            files_data.extend(self._process_local_path(path))

        # 2. http / https
        elif uri.startswith("http://") or uri.startswith("https://"):
            files_data.append(self._process_http(uri))

        # 3. Windows 绝对路径 (C:\ or C:/)
        elif self._is_windows_path(uri):
            files_data.extend(self._process_local_path(uri))

        # 4. Linux/macOS 绝对路径 (/data/xxx)
        elif uri.startswith("/"):
            files_data.extend(self._process_local_path(uri))

        else:
            raise ValueError(f"Unsupported or non-existing URI: {uri}")

        # 注入 user_metadata
        if self.user_metadata:
            for f in files_data:
                f["user_metadata"] = self.user_metadata

        return files_data

    # --------------------------
    # 本地路径处理
    # --------------------------
    def _process_local_path(self, path: str) -> List[Dict[str, Any]]:
        path = os.path.abspath(path)
        logger.info(f"[URISource] Processing local path: {path}")

        if not os.path.exists(path):
            raise ValueError(f"Local path does not exist: {path}")

        results = []

        # 文件
        if os.path.isfile(path):
            results.append(self._load_local_file(path))

        # 文件夹
        elif os.path.isdir(path):
            for root, _, files in os.walk(path):
                for f in files:
                    full_path = os.path.join(root, f)
                    try:
                        results.append(self._load_local_file(full_path))
                    except Exception as e:
                        logger.error(f"[URISource] Error reading {full_path}: {e}")
        else:
            raise ValueError(f"Invalid path (not file or directory): {path}")

        return results

    def _load_local_file(self, path: str) -> Dict[str, Any]:
        full_path = os.path.abspath(path)
        file_name = os.path.basename(full_path)
        logger.info(f"[URISource] Loading local file: {file_name}")
        with open(path, "rb") as f:
            binary = f.read()

        return {
            "file_name": file_name,
            "binary": binary,
            "source_path": full_path,
            "source_type": "uri"
        }

    # --------------------------
    # HTTP/HTTPS 下载处理
    # --------------------------
    def _process_http(self, url: str) -> Dict[str, Any]:
        """
        下载 HTTP/HTTPS 文件，同时返回 source_path（原始 URL）。
        如果 URL 没有明显文件名，用 'remote_file' 或 URL 中最后一段清理生成。
        """
        logger.info(f"[URISource] Downloading URL: {url}")
        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            raise ValueError(f"Failed to download {url}: {e}")

        # 尝试从 URL 获取文件名
        filename = url.split("/")[-1].split("?")[0] or "remote_file"
        # 清理文件名，去掉非法字符
        filename = re.sub(r'[^\w\.\-]+', '_', filename)

        return {
            "file_name": filename,
            "binary": resp.content,
            "source_path": url,  # 用 URL 作为 source_path
            "source_type": "uri"
        }

    # --------------------------
    # 工具函数：判断 Windows 路径
    # --------------------------
    def _is_windows_path(self, uri: str) -> bool:
        return (len(uri) > 2 and uri[1] == ":" and (uri[2] == "\\" or uri[2] == "/"))
