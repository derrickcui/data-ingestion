# app/api/routes/file_ingest.py
from fastapi import APIRouter, UploadFile, File, HTTPException

# 导入 celery 任务（可能是占位）
from app.worker.tasks import ingest_file_task

# 导入同步 runner builder
from app.orchestrator.pipeline_runner import PipelineRunner
from app.sources.file_source import FileSource
from app.pipelines.tika_processor import TikaProcessor
from app.pipelines.clean_processor import CleanProcessor
from app.pipelines.chunk_processor import ChunkProcessor
from app.pipelines.llm_processor import LLMProcessor
from app.pipelines.embed_processor import EmbedProcessor
from app.sinks.solr_sink import SolrSink
from app.sinks.chroma_sink import ChromaSink

router = APIRouter()

def _make_runner(filename: str, content: bytes):
    source = FileSource(filename, content)
    processors = [
        TikaProcessor(),
        CleanProcessor(),
        ChunkProcessor(),
        LLMProcessor(),
        EmbedProcessor(),
    ]
    sinks = [SolrSink(), ChromaSink()]
    return PipelineRunner(source=source, processors=processors, sinks=sinks)

@router.post("/upload_async")
async def upload_async(file: UploadFile = File(...)):
    """
    异步模式：需要配置 REDIS_BROKER；会把任务投递给 Celery worker。
    """
    # 读取 bytes（避免 decode 问题）
    content = await file.read()

    # 检查是否配置了 Celery
    from app.worker.celery_app import celery_app
    if celery_app is None:
        raise HTTPException(status_code=500, detail="Async mode not configured (REDIS_BROKER missing).")

    # 使用 Celery 任务入队
    task = ingest_file_task.delay(file.filename, content)
    return {"status": "queued", "task_id": task.id}

@router.post("/upload_sync")
async def upload_sync(file: UploadFile = File(...)):
    """
    同步本地模式：直接在请求里执行 Pipeline（适合本地/调试/小数据量）。
    """
    content = await file.read()
    runner = _make_runner(file.filename, content)
    try:
        result = runner.run()
        return {"status": "ok", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
