# app/api/routes/file_ingest.py

from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from typing import Optional

from app.ai_providers.aliyun_llm_client import AliyunLLMClient
from app.ai_providers.google_llm_client import GoogleLLMClient
from app.ai_providers.openai_llm_client import OpenAILLMClient
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

router = APIRouter()

def _make_runner(
    filename: str,
    content: bytes,
    embedding_client: Optional[object] = None,  # 可传 OpenAI/ALI/Google client
    llm_client: Optional[object] = None  # 可传 OpenAI/ALI/Google LLM client
):
    source = FileSource(filename, content)
    sinks = [SolrSink(), ChromaSink()]

    processor_classes = load_all_processor_classes()
    processors = []
    for cls in processor_classes:
        # 只有 EmbedProcessor 需要 client，其它 processor 无参构造
        if cls.__name__ == "EmbedProcessor" and embedding_client is not None:
            processors.append(cls(client=embedding_client))
        elif cls.__name__ == "LLMProcessor" and llm_client is not None:
            processors.append(cls(client=llm_client))
        else:
            processors.append(cls())

    runner = PipelineRunner(source, processors, sinks)
    return runner

# ------------------ 同步接口 ------------------

@router.post("/upload_sync",
             summary="同步上传文件并启动处理流程",
             response_description="返回处理任务的状态或ID"
)
async def upload_sync(
        file: UploadFile = File(..., description="要上传和处理的文档文件"),
        # 强制将 provider 识别为 URL 查询参数
        provider: Optional[str] = Query(
            None,
            description="用于文件上传或处理的服务提供商标识 (例如: 'Azure', 'S3')",
            example="Azure"
        ),
        # 强制将 api_key 识别为 URL 查询参数
        api_key: Optional[str] = Query(
            None,
            description="用于鉴权或连接外部服务的 API 密钥",
            example="sk-xxxxxxxx"
        )
):
    """
    同步上传：直接执行 pipeline，适合调试/小数据量
    provider + api_key 可选择使用大模型生成 embedding
    """
    content = await file.read()

    # 根据 provider 初始化 embedding client
    embedding_client = None
    llm_client = None
    if provider and api_key:
        provider = provider.lower()
        if provider == "openai":
            embedding_client = OpenAIEmbeddingClient(api_key)
            llm_client = OpenAILLMClient(api_key)
        elif provider == "ali":
            embedding_client = AliEmbeddingClient(api_key)
            llm_client = AliyunLLMClient(api_key)
        elif provider == "google":
            embedding_client = GoogleEmbeddingClient(api_key)
            llm_client = GoogleLLMClient(api_key)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown provider: {provider}")

    runner = _make_runner(file.filename, content, embedding_client, llm_client)
    try:
        result = runner.run()
        return {"status": "ok", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ------------------ 异步接口 ------------------
@router.post("/upload_async")
async def upload_async(file: UploadFile = File(...)):
    """
    异步上传：使用 Celery，将任务投递给 worker
    """
    content = await file.read()

    from app.worker.celery_app import celery_app
    if celery_app is None:
        raise HTTPException(status_code=500, detail="Async mode not configured (REDIS_BROKER missing).")

    task = ingest_file_task.delay(file.filename, content)
    return {"status": "queued", "task_id": task.id}
