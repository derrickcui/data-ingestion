# app/worker/tasks.py
from app.worker.celery_app import celery_app
from app.orchestrator.pipeline_runner import PipelineRunner
from app.sources.file_source import FileSource
from app.pipelines.tika_processor import TikaProcessor
from app.pipelines.clean_processor import CleanProcessor
from app.pipelines.chunk_processor import ChunkProcessor
from app.pipelines.llm_processor import LLMProcessor
from app.pipelines.embed_processor import EmbedProcessor
from app.sinks.solr_sink import SolrSink
from app.sinks.chroma_sink import ChromaSink

# 如果 celery_app 是 None（没有 Redis），导出一个占位函数以避免导入错误
if celery_app is None:
    def ingest_file_task_placeholder(filename: str, content: bytes):
        raise RuntimeError("Celery not configured. Set REDIS_BROKER to enable async mode.")
    ingest_file_task = ingest_file_task_placeholder
else:
    @celery_app.task(bind=True, name="ingest_file_task")
    def ingest_file_task(self, filename: str, content: bytes):
        try:
            runner = PipelineRunner(
                source=FileSource(filename, content),
                processors=[
                    TikaProcessor(),
                    CleanProcessor(),
                    ChunkProcessor(),
                    LLMProcessor(),
                    EmbedProcessor(),
                ],
                sinks=[SolrSink(), ChromaSink()]
            )
            result = runner.run()
            return {"status": "success", "meta": {"chunks": len(result.get("chunks", []))}}
        except Exception as e:
            # 自动重试机制
            raise self.retry(exc=e, countdown=10, max_retries=3)
