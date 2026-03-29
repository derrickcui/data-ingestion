from app.ai_vocabulary.services import AIVocabularyRunService
from app.db.session import SessionLocal
from app.orchestrator.pipeline_runner import PipelineRunner
from app.pipelines.chunk_processor import ChunkProcessor
from app.pipelines.clean_processor import CleanProcessor
from app.pipelines.embed_processor import EmbedProcessor
from app.pipelines.tika_processor import TikaProcessor
from app.sinks.chroma_sink import ChromaSink
from app.sinks.solr_sink import SolrSink
from app.sources.file_source import FileSource
from app.worker.celery_app import celery_app


if celery_app is None:
    def ingest_file_task_placeholder(filename: str, content: bytes):
        raise RuntimeError("Celery not configured. Set REDIS_BROKER to enable async mode.")

    def execute_ai_vocabulary_run_task_placeholder(run_id: str):
        raise RuntimeError("Celery not configured. Set REDIS_BROKER to enable async mode.")

    ingest_file_task = ingest_file_task_placeholder
    execute_ai_vocabulary_run_task = execute_ai_vocabulary_run_task_placeholder
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
                    EmbedProcessor(),
                ],
                sinks=[SolrSink(), ChromaSink()],
            )
            result = runner.run()
            return {"status": "success", "meta": {"chunks": len(result.get("chunks", []))}}
        except Exception as exc:
            raise self.retry(exc=exc, countdown=10, max_retries=3)

    @celery_app.task(bind=True, name="execute_ai_vocabulary_run_task")
    def execute_ai_vocabulary_run_task(self, run_id: str):
        db = SessionLocal()
        try:
            service = AIVocabularyRunService(db)
            run = service.execute_run(run_id)
            return {"status": "success", "run_id": run.id, "total_terms": run.total_terms}
        except Exception as exc:
            raise self.retry(exc=exc, countdown=10, max_retries=3)
        finally:
            db.close()
