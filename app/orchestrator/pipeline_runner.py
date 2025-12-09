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
    返回轻量级 summary 而非全量 embedding payload。
    """

    def __init__(self, source: BaseSource,
                 processors: List[BaseProcessor],
                 sinks: List[BaseSink] = None,
                 max_workers: int = 10):
        self.source = source
        self.processors = sorted(processors, key=lambda p: getattr(p, "order", 100))
        self.sinks = sinks or []
        self.max_workers = max_workers

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
                logger.error(f"[PipelineRunner] Processor {processor.__class__.__name__} failed for "
                             f"file {data.get('file_name')}: {e}")
                raise RuntimeError(f"Pipeline aborted due to failure in processor "
                                   f"{processor.__class__.__name__}") from e

        for sink in self.sinks:
            try:
                sink.write(data, context=context)
            except Exception as e:
                logger.error(f"[PipelineRunner] Sink {sink.__class__.__name__} failed for "
                             f"file {data.get('file_name')}: {e}")
                raise

        # ✅ 只返回 summary，内部 full data 保留
        return self._build_summary(data)

    def run(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        context = context or {}

        # 1) Source read
        data_or_list = self.source.read(context=context)

        # 2) 判断是单文件 dict 还是多文件 list
        if isinstance(data_or_list, list):
            summaries = []
            # 并发处理多文件
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_file = {
                    executor.submit(self.run_single, d, context): d.get("file_name")
                    for d in data_or_list
                }
                for future in as_completed(future_to_file):
                    file_name = future_to_file[future]
                    try:
                        summaries.append(future.result())
                    except Exception as e:
                        logger.error(f"[PipelineRunner] Processing failed for file {file_name}: {e}")
                        summaries.append({
                            "file_name": file_name,
                            "status": "failed",
                            "error": str(e),
                        })

            return {
                "status": "completed",
                "total_files": len(summaries),
                "files": summaries
            }

        # 单文件
        elif isinstance(data_or_list, dict):
            return {
                "status": "completed",
                "total_files": 1,
                "files": [self.run_single(data_or_list, context=context)]
            }

        else:
            raise TypeError(
                f"Source.read() must return dict or list of dict, got {type(data_or_list)}"
            )

    # -----------------------------
    #   Summary Builder
    # -----------------------------
    def _build_summary(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        从 full pipeline data 中构造 API 返回安全结果
        """
        chunks = data.get("chunks") or []
        embeddings = data.get("embeddings") or []

        return {
            "file_name": data.get("file_name"),
            "doc_id": data.get("doc_id"),
            "status": data.get("status", "ok"),
            "chunk_count": len(chunks),
            "embedding_count": len(embeddings),
            "embedding_dim": (
                len(embeddings[0].get("embedding", []))
                if embeddings
                else 0
            ),
            "source": data.get("source"),
            "elapsed_ms": data.get("elapsed_ms"),
        }
