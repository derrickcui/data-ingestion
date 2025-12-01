# app/api/routes/file_ingest.py

from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from typing import Optional

from app.ai_providers.aliyun_llm_client import AliyunLLMClient
from app.ai_providers.google_llm_client import GoogleLLMClient
from app.ai_providers.openai_llm_client import OpenAILLMClient
from app.utility.config import Config
from app.worker.tasks import ingest_file_task
from app.orchestrator.pipeline_runner import PipelineRunner
from app.sources.file_source import FileSource
from app.sinks.solr_sink import SolrSink
from app.sinks.chroma_sink import ChromaSink
from app.pipelines.processor_registry import load_all_processor_classes
from app.ai_providers.openai_client import OpenAIEmbeddingClient
from app.ai_providers.aliyun_client import AliEmbeddingClient
from app.ai_providers.google_client import GoogleEmbeddingClient
from app.utility.log import logger

from concurrent.futures import ThreadPoolExecutor
from fastapi.concurrency import run_in_threadpool

router = APIRouter()

# 全局线程池（50并发足够）
executor = ThreadPoolExecutor(max_workers=50)


# -------------------------------------------------
#  构建 Pipeline Runner
# -------------------------------------------------
def _make_runner(
    filename: str,
    content: bytes,
    embedding_client: Optional[object] = None,
    llm_client: Optional[object] = None
):
    source = FileSource(filename, content)
    sinks = [SolrSink()]

    processor_classes = load_all_processor_classes()
    processors = []
    for cls in processor_classes:
        if cls.__name__ == "EmbedProcessor" and embedding_client is not None:
            processors.append(cls(client=embedding_client))
        elif cls.__name__ == "LLMProcessor" and llm_client is not None:
            processors.append(cls(client=llm_client))
        else:
            processors.append(cls())

    runner = PipelineRunner(source, processors, sinks)
    return runner


# -------------------------------------------------
#  同步接口（已改成真正的多并发）
# -------------------------------------------------
@router.post("/upload_sync",
             summary="同步上传文件并启动处理流程（多并发）",
             response_description="返回处理结果"
)
async def upload_sync(
        file: UploadFile = File(...),
        provider: Optional[str] = Query(
            None,
            description="embedding/LLM provider，如 'ali', 'openai', 'google'",
            example="ali"
        )
):
    """同步上传 + 在线执行 pipeline（线程池执行，可并发）"""

    content = await file.read()

    # 根据 provider 初始化 embedding & LLM client
    embedding_client = None
    llm_client = None

    if provider:
        provider = provider.lower()
        if provider == "openai":
            embedding_client = OpenAIEmbeddingClient(Config.OPENAI_API_KEY)
            llm_client = OpenAILLMClient(Config.OPENAI_API_KEY)
        elif provider == "ali":
            embedding_client = AliEmbeddingClient(Config.ALI_QWEN_API_KEY)
            llm_client = AliyunLLMClient(Config.ALI_QWEN_API_KEY)
        elif provider == "google":
            embedding_client = GoogleEmbeddingClient(Config.GOOGLE_API_KEY)
            llm_client = GoogleLLMClient(Config.GOOGLE_API_KEY)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown provider: {provider}")

    runner = _make_runner(file.filename, content, embedding_client, llm_client)

    try:
        # ✨ 用 threadpool 执行 runner.run（解决阻塞，实现高并发）
        result = await run_in_threadpool(runner.run)
        return {"status": "ok", "result": result}

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------------------------------
#  异步接口（Celery 版本）
# -------------------------------------------------
@router.post("/upload_async")
async def upload_async(file: UploadFile = File(...)):
    """异步上传：使用 Celery，将任务投递给 worker"""
    content = await file.read()

    from app.worker.celery_app import celery_app
    if celery_app is None:
        raise HTTPException(status_code=500, detail="Async mode not configured (REDIS_BROKER missing).")

    task = ingest_file_task.delay(file.filename, content)
    return {"status": "queued", "task_id": task.id}
