# app/orchestrator/pipeline_runner.py
from typing import Dict, Any, List, Optional
from app.pipelines.base import BaseProcessor
from app.sources.base import BaseSource
from app.sinks.base import BaseSink
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logger = logging.getLogger(__name__)


class PipelineRunner:
    """
    Orchestrator: Source -> Processors -> Sinks
    支持单文件和多文件列表（适配目录上传）
    """

    def __init__(self, source: BaseSource, processors: List[BaseProcessor], sinks: List[BaseSink] = None, max_workers: int = 10):
        self.source = source
        self.processors = sorted(processors, key=lambda p: getattr(p, "order", 100))
        self.sinks = sinks or []
        self.max_workers = max_workers  # 并发处理文件的线程数

    def run_single(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None):
        """
        处理单个文件（dict），按 processors 顺序处理，并写入 sinks
        """
        context = context or {}

        # 顺序处理 processors
        for processor in self.processors:
            try:
                out = processor.process(data, context=context) or {}
                if not isinstance(out, dict):
                    raise TypeError(f"Processor {processor.__class__.__name__} must return dict, got {type(out)}")
                data.update(out)
            except Exception as e:
                logger.error(f"[PipelineRunner] Processor {processor.__class__.__name__} failed for file {data.get('file_name')}: {e}")
                raise RuntimeError(f"Pipeline aborted due to failure in processor {processor.__class__.__name__}") from e

        # sinks 写入
        for sink in self.sinks:
            try:
                sink.write(data, context=context)
            except Exception as e:
                logger.error(f"[PipelineRunner] Sink {sink.__class__.__name__} failed for file {data.get('file_name')}: {e}")
                raise

        return data

    def run(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        context = context or {}

        # 1) Source read
        data_or_list = self.source.read(context=context)

        # 2) 判断是单文件 dict 还是多文件 list
        if isinstance(data_or_list, list):
            results = []
            # 并发处理多文件
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_file = {executor.submit(self.run_single, d, context): d.get("file_name") for d in data_or_list}
                for future in as_completed(future_to_file):
                    file_name = future_to_file[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"[PipelineRunner] Processing failed for file {file_name}: {e}")
            return {"files": results}

        elif isinstance(data_or_list, dict):
            return self.run_single(data_or_list, context=context)

        else:
            raise TypeError(f"Source.read() must return dict or list of dict, got {type(data_or_list)}")
