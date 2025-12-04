from fastapi import APIRouter, UploadFile, File, HTTPException, Query, Form
from typing import Optional
import json  # 导入 json 库用于解析元数据

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
#   构建 Pipeline Runner
# -------------------------------------------------
def _make_runner(
        filename: str,
        content: bytes,
        metadata: Optional[dict] = None,  # <-- 新增：接收解析后的元数据字典
        embedding_client: Optional[object] = None,
        llm_client: Optional[object] = None
):
    # 1. 初始化 Source
    source = FileSource(filename, content)

    # 2. 核心修改：将元数据附加到 Source 对象上
    # 假设 FileSource 类有一个 metadata 属性可以存储额外信息

    if metadata:
        source.user_metadata = metadata
        logger.info(f"Attached metadata to file source: {metadata}")

    # 3. 初始化 Sink
    sinks = [SolrSink(Config.SOLR_URL, Config.SOLR_COLLECTION)]

    # 4. 初始化 Processors
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
#   同步接口（已改成真正的多并发）
# -------------------------------------------------
@router.post("/upload_sync",
             summary="同步上传文件及元数据并启动处理流程（多并发）",
             response_description="返回处理结果"
             )
async def upload_sync(
        file: UploadFile = File(...),
        metadata: Optional[str] = Form(
            None,
            description="可选的JSON格式元数据字符串，例如: '{\"author\": \"Jane Doe\"}'",  # <-- 新增：接收 JSON 字符串
            example='{"document_type": "report", "year": 2023}'
        ),
        provider: Optional[str] = Query(
            None,
            description="embedding/LLM provider，如 'ali', 'openai', 'google'",
            example="ali"
        )
):
    """同步上传 + 在线执行 pipeline（线程池执行，可并发），支持同时上传 JSON 格式元数据。"""

    content = await file.read()

    # 解析 metadata 字符串
    parsed_metadata = {}
    if metadata:
        try:
            parsed_metadata = json.loads(metadata)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid metadata JSON: {metadata}. Error: {e}")
            raise HTTPException(status_code=400, detail=f"Metadata must be valid JSON: {e}")

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

    # 传递解析后的元数据给 runner
    runner = _make_runner(file.filename, content, parsed_metadata, embedding_client, llm_client)

    try:
        # ✨ 用 threadpool 执行 runner.run（解决阻塞，实现高并发）
        result = await run_in_threadpool(runner.run)
        return {"status": "ok", "result": result}

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise HTTPException(status_code=500, detail=f"Pipeline processing failed: {e}")


# -------------------------------------------------
#   异步接口（Celery 版本） - 建议在 task.delay 中也添加 metadata 参数
# -------------------------------------------------
@router.post("/upload_async")
async def upload_async(file: UploadFile = File(...), metadata: Optional[str] = Form(None)):
    """异步上传：使用 Celery，将任务投递给 worker"""
    content = await file.read()

    # 解析 metadata 字符串
    parsed_metadata = {}
    if metadata:
        try:
            parsed_metadata = json.loads(metadata)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"Metadata must be valid JSON: {e}")

    from app.worker.celery_app import celery_app
    if celery_app is None:
        raise HTTPException(status_code=500, detail="Async mode not configured (REDIS_BROKER missing).")

    # 注意：你需要确保 ingest_file_task 也能接受并处理 metadata
    task = ingest_file_task.delay(file.filename, content, parsed_metadata)
    return {"status": "queued", "task_id": task.id}