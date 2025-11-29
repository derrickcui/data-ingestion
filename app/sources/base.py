# app/sources/base.py
from typing import Dict, Any, Optional


class BaseSource:
    """
    Source 基类：所有数据来源（文件/数据库/API）请继承此类并实现 read().
    """

    def read(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        读取数据并返回字典。
        context: pipeline 运行时上下文（可选）
        返回格式示例：
            {
                "filename": "...",
                "binary": b"...",
                ...
            }
        """
        raise NotImplementedError("BaseSource.read must be implemented by subclasses")
