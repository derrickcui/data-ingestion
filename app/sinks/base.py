# app/sinks/base.py
from typing import Dict, Any, Optional


class BaseSink:
    """
    Sink 基类。所有 sink（写入 Solr/Chroma/DB）继承此类并实现 write().
    """

    def write(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> None:
        """
        将 pipeline 最终数据写入目标。
        不应返回数据；如需报告状态可把状态写到日志或外部监控系统。
        """
        raise NotImplementedError("BaseSink.write must be implemented by subclasses")
