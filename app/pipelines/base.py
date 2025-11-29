# app/pipelines/base.py
from typing import Dict, Any, Optional


class BaseProcessor:
    """
    Processor 基类。所有处理器继承自此类并实现 process 方法。
    order 决定了 processor 的执行顺序（从小到大）。
    """

    order: int = 100

    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        接收上游 data，返回要 merge 回 pipeline data 的 dict。
        注意：不要就地修改传入的字典（除非你明确想这样做）。
        返回值可以是 {} 表示没有新字段。
        """
        raise NotImplementedError("BaseProcessor.process must be implemented by subclasses")
