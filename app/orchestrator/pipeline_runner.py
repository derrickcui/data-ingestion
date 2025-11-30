# app/orchestrator/pipeline_runner.py
from typing import Dict, Any, List, Optional

from app.pipelines.base import BaseProcessor
from app.sources.base import BaseSource
from app.sinks.base import BaseSink


class PipelineRunner:
    """
    Orchestrator: Source -> Processors -> Sinks
    使用约定的 Base* 接口。
    """

    def __init__(self, source: BaseSource, processors: List[BaseProcessor], sinks: List[BaseSink] = None):
        self.source = source
        # 确保 processors 是按 order 排序的（如果传入的 redan 排序，这里也会二次排序）
        self.processors = sorted(processors, key=lambda p: getattr(p, "order", 100))
        self.sinks = sinks or []

    def run(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        context = context or {}

        # 1) Source read
        data = self.source.read(context=context)

        # 2) processors 顺序处理，每个 processor 返回 dict，将其 merge 到 data 中
        for processor in self.processors:
            try:
                out = processor.process(data, context=context) or {}
                if not isinstance(out, dict):
                    raise TypeError(f"Processor {processor.__class__.__name__} must return dict, got {type(out)}")
                # 建议不在原地修改 data（但若 processor 返回的字段与原 data 冲突，会覆盖）
                data.update(out)
            except Exception as e:
                # 打印或记录错误
                print(f"[PipelineRunner] Processor {processor.__class__.__name__} failed: {e}")
                # 立即中止 Pipeline
                raise RuntimeError(f"Pipeline aborted due to failure in processor {processor.__class__.__name__}") from e

        # 3) sinks 写入
        for sink in self.sinks:
            sink.write(data, context=context)

        return data