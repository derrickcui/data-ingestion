from fastapi import APIRouter, UploadFile, File, HTTPException, Query, Form
from typing import Optional
import json
import base64
import requests

from app.ai_providers.aliyun_llm_client import AliyunLLMClient
from app.ai_providers.google_llm_client import GoogleLLMClient
from app.ai_providers.openai_llm_client import OpenAILLMClient
from app.utility.config import Config
from app.orchestrator.pipeline_runner import PipelineRunner
from app.sources.file_source import FileSource
from app.sources.text_source import TextSource
from app.sources.uri_source import URISource
from app.sources.base64_source import Base64Source
from app.sinks.solr_sink import SolrSink
from app.pipelines.processor_registry import load_all_processor_classes
from app.ai_providers.openai_client import OpenAIEmbeddingClient
from app.ai_providers.aliyun_client import AliEmbeddingClient
from app.ai_providers.google_client import GoogleEmbeddingClient
from app.utility.log import logger

from concurrent.futures import ThreadPoolExecutor
from fastapi.concurrency import run_in_threadpool

router = APIRouter()

# 全局线程池（50并发）
executor = ThreadPoolExecutor(max_workers=50)


# -------------------------------------------------
#   构建 Pipeline Runner（保持与你的版本一致）
# -------------------------------------------------
def _make_runner(
        filename: str,
        content,
        metadata: Optional[dict] = None,
        embedding_client: Optional[object] = None,
        llm_client: Optional[object] = None,
        source_type: str = "file"
):
    """
    根据不同 source_type 生成不同 Source 对象
    """

    # 1. Source 选择
    if source_type == "file":
        source = FileSource(filename, content)

    elif source_type == "text":
        source = TextSource(text=content)

    elif source_type == "uri":
        source = URISource(uri=content)

    elif source_type == "base64":
        source = Base64Source(filename, content)

    else:
        raise ValueError(f"Unsupported source_type: {source_type}")

    # 2. 加入 metadata
    if metadata:
        source.user_metadata = metadata
        logger.info(f"Attached metadata to file source: {metadata}")

    # 3. Sink
    sinks = [SolrSink()]

    # 4. Processors
    processor_classes = load_all_processor_classes()
    processors = []

    # 确定是否需要 Tika 解析
    # 只有 FileSource, URISource, Base64Source (需要解压/解析原始文档) 才需要 Tika
    should_run_tika = source_type in ["file", "uri", "base64"]

    for cls in processor_classes:
        processor_name = cls.__name__

        # ⭐️ 动态跳过 TikaProcessor 逻辑 ⭐️
        if processor_name == "TikaProcessor" and not should_run_tika:
            logger.info(f"Skipping {processor_name} for source_type='{source_type}'")
            continue
        # ------------------------------------

        if processor_name == "EmbedProcessor" and embedding_client:
            processors.append(cls(client=embedding_client))
        elif processor_name == "LLMProcessor" and llm_client:
            processors.append(cls(client=llm_client))
        else:
            processors.append(cls())

    return PipelineRunner(source, processors, sinks)


# -------------------------------------------------
#   统一数据摄取接口（文本/文件/URI/Base64）
# -------------------------------------------------
@router.post("/ingest", summary="统一数据摄取入口")
async def ingest(
        source_type: str = Form(..., description="file | text | uri | base64"),

        # file
        file: Optional[UploadFile] = File(None),

        # text
        text: Optional[str] = Form(None),

        # uri
        uri: Optional[str] = Form(None),

        # base64
        base64_content: Optional[str] = Form(None),

        # metadata
        metadata: Optional[str] = Form(None),

        # provider
        provider: Optional[str] = Query(None)
):
    """
    一个接口支持所有类型的内容摄取。
    处理逻辑与 upload_sync 保持一致：解析 metadata、provider，进入 pipeline。
    """
    # -------------------------
    # 解析 metadata JSON
    # -------------------------
    parsed_metadata = {}
    if metadata:
        try:
            parsed_metadata = json.loads(metadata)
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Metadata must be valid JSON: {e}"
            )

    # -------------------------
    # provider 选择
    # -------------------------
    embedding_client = None
    llm_client = None

    if provider:
        p = provider.lower()
        if p == "openai":
            embedding_client = OpenAIEmbeddingClient(Config.OPENAI_API_KEY)
            llm_client = OpenAILLMClient(Config.OPENAI_API_KEY)
        elif p == "ali":
            embedding_client = AliEmbeddingClient(Config.ALI_QWEN_API_KEY)
            llm_client = AliyunLLMClient(Config.ALI_QWEN_API_KEY)
        elif p == "google":
            embedding_client = GoogleEmbeddingClient(Config.GOOGLE_API_KEY)
            llm_client = GoogleLLMClient(Config.GOOGLE_API_KEY)
        else:
            raise HTTPException(400, f"Unknown provider: {provider}")

    # -------------------------
    # 根据 source_type 提取内容
    # -------------------------
    if source_type == "file":
        if not file:
            raise HTTPException(400, "file is required for source_type=file")
        filename = file.filename or "uploaded_file"
        content = await file.read()

    elif source_type == "text":
        if not text:
            raise HTTPException(400, "text is required for source_type=text")
        filename = "inline_text"
        content = text

    elif source_type == "uri":
        if not uri:
            raise HTTPException(400, "uri is required for source_type=uri")
        content = uri
        filename = uri.split("/")[-1].split("?")[0].split("#")[0] or "remote_file"

    elif source_type == "base64":
        if not base64_content:
            raise HTTPException(400, "base64_content required for base64 source")
        content = base64_content.encode("utf-8")
        filename = "base64_input"

    else:
        raise HTTPException(400, f"Invalid source_type: {source_type}")

    # -------------------------
    # 构造 runner（保持与 upload_sync 一致）
    # -------------------------
    runner = _make_runner(
        filename=filename,
        content=content,
        metadata=parsed_metadata,
        embedding_client=embedding_client,
        llm_client=llm_client,
        source_type=source_type
    )

    # -------------------------
    # ThreadPool 运行 pipeline（同步接口）
    # -------------------------
    try:
        result = await run_in_threadpool(runner.run)
        return {"status": "ok", "result": result}

    except Exception as e:
        logger.error(f"Ingest pipeline failed: {e}")
        raise HTTPException(status_code=500, detail=f"Pipeline failed: {e}")
