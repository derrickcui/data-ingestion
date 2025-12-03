from fastapi import APIRouter, UploadFile, File, HTTPException, Query, Form
from typing import Optional, Dict, Any, Union, List
import json
import base64
import requests
from pydantic import BaseModel, Field

# 导入所有内部依赖（假设这些类已定义且可用）
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
#   Pydantic 模型：用于 /ingest (JSON Body)
# -------------------------------------------------
class IngestStructuredRequest(BaseModel):
    """用于接收结构化文本、URI或Base64数据的JSON请求体"""

    # 强制要求指定一种类型
    source_type: str = Field(..., description="text | uri | base64")

    # 互斥的内容字段
    text: Optional[str] = Field(None, description="当 source_type=text 时传入纯文本")
    uri: Optional[str] = Field(None, description="当 source_type=uri 时传入远程资源的URL")
    base64_content: Optional[str] = Field(None, description="当 source_type=base64 时传入Base64编码的字节流")

    # 元数据和 Provider
    metadata: Optional[Dict[str, Any]] = Field(None, description="可选的业务元数据对象")
    provider: Optional[str] = Field(None, description="embedding/LLM provider，如 ali, openai, google")
    source_system: Optional[str] = Field(None, description="来源系统标识，用于 Doc ID 生成") # <-- ADDED

    # 确保只传入一个内容字段
    @classmethod
    def __pydantic_validator__(cls, values):
        # 注意: 在 Pydantic V2 中，应该使用 @model_validator(mode='before') 或 @field_validator
        # 但为了保持代码兼容性，我们保留这个 classmethod 验证
        if values.get('source_type') == 'text' and not values.get('text'):
            raise ValueError("text is required when source_type is 'text'")
        if values.get('source_type') == 'uri' and not values.get('uri'):
            raise ValueError("uri is required when source_type is 'uri'")
        if values.get('source_type') == 'base64' and not values.get('base64_content'):
            raise ValueError("base64_content is required when source_type is 'base64'")
        return values


# -------------------------------------------------
#   构建 Pipeline Runner（核心逻辑：移除 Tika 动态跳过）
# -------------------------------------------------
def _make_runner(
        filename: str,
        content: Union[str, bytes],  # content 可以是 str (text/uri) 也可以是 bytes (file/base64)
        metadata: Optional[dict] = None,
        embedding_client: Optional[object] = None,
        llm_client: Optional[object] = None,
        source_type: str = "file",
        source_system: Optional[str] = None # <-- ADDED
):
    """根据不同 source_type 生成不同 Source 对象并配置 PipelineRunner"""

    logger.info(f"Starting pipeline runner for source_type='{source_type}' and filename='{filename}'")

    # 1. Source 选择
    if source_type == "file":
        source = FileSource(filename, content)  # content is bytes

    elif source_type == "text":
        source = TextSource(text=content)  # content is str

    elif source_type == "uri":
        source = URISource(uri=content)  # content is str

    elif source_type == "base64":
        source = Base64Source(filename, content)  # content is bytes (encoded string)

    else:
        # 这个分支理论上不应该在 API 层面触发，因为已被验证
        raise ValueError(f"Unsupported source_type: {source_type}")

    # 2. 加入 metadata
    final_metadata = metadata.copy() if metadata else {}
    if source_system:
        # 将 source_system 注入到 metadata 中，供 IdProcessor 读取
        final_metadata["source_system"] = source_system
        logger.info(f"Attached source_system: {source_system} to metadata.")

    if final_metadata:
        # 假设所有 Source 类都有 user_metadata 属性，并直接赋值。
        source.user_metadata = final_metadata
        logger.info(f"Attached metadata to source: {final_metadata}")


    # 3. Sink
    sinks = [SolrSink()]

    # 4. Processors
    processor_classes = load_all_processor_classes()
    processors = []

    # 统一处理所有 Processor：
    for cls in processor_classes:
        processor_name = cls.__name__

        if processor_name == "EmbedProcessor" and embedding_client:
            processors.append(cls(client=embedding_client))
        elif processor_name == "LLMProcessor" and llm_client:
            processors.append(cls(client=llm_client))
        else:
            processors.append(cls())

    return PipelineRunner(source, processors, sinks)


# -------------------------------------------------
#   助手函数：初始化 LLM 和 Embedding 客户端
# -------------------------------------------------
def _initialize_clients(provider: Optional[str]):
    """根据 provider 初始化 LLM 和 Embedding 客户端"""
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

    return embedding_client, llm_client


# -------------------------------------------------
#   1. 专用文件上传接口 (Multipart Form)
# -------------------------------------------------
@router.post("/upload", summary="专用文件上传接口（File Ingestion）")
async def upload(
        file: UploadFile = File(..., description="要上传的文档文件"),
        metadata: Optional[str] = Form(None, description='可选 JSON 格式元数据字符串'),
        provider: Optional[str] = Query(None, description="embedding/LLM provider"),
        source_system: Optional[str] = Query(None, description="来源系统标识，用于 Doc ID 生成") # <-- ADDED
):
    """
    接收文件和可选元数据，进行同步摄取。
    Content-Type 必须是 multipart/form-data。
    """

    # 1. 解析 Metadata
    parsed_metadata = {}
    if metadata:
        try:
            parsed_metadata = json.loads(metadata)
        except Exception as e:
            raise HTTPException(400, detail=f"Metadata must be valid JSON: {e}")

    # 2. 初始化 Client
    embedding_client, llm_client = _initialize_clients(provider)

    # 3. 提取内容
    filename = file.filename or "uploaded_file"
    content = await file.read()  # Content is bytes

    # 4. 构造 Runner (source_type="file")
    runner = _make_runner(
        filename=filename,
        content=content,
        metadata=parsed_metadata,
        embedding_client=embedding_client,
        llm_client=llm_client,
        source_type="file",
        source_system=source_system # <-- PASSED
    )

    # 5. ThreadPool 运行
    try:
        result = await run_in_threadpool(runner.run)
        return {"status": "ok", "result": result}
    except Exception as e:
        logger.error(f"Upload pipeline failed: {e}")
        raise HTTPException(status_code=500, detail=f"Pipeline failed: {e}")


# -------------------------------------------------
#   2. 统一结构化数据摄取接口 (JSON Body)
# -------------------------------------------------
@router.post("/ingest", summary="统一结构化数据摄取入口（批量 Text/URI/Base64）")
async def ingest_structured(requests: List[IngestStructuredRequest]):
    """
    接收结构化 JSON Body 数组，批量处理文本、URI或Base64内容。
    Content-Type 必须是 application/json。
    """

    if not requests:
        return {"status": "completed", "total_requests": 0, "results": []}

    all_results = []

    # 循环处理数组中的每一个请求
    for req in requests:
        source_type = req.source_type

        # 1. 初始化 Client
        try:
            # 客户端在循环内初始化，确保每个请求都能正确配置 provider
            embedding_client, llm_client = _initialize_clients(req.provider)
        except HTTPException as e:
            # 记录并跳过失败的请求
            all_results.append({"status": "failed", "source_type": source_type, "error": str(e.detail)})
            logger.error(f"Client initialization failed for {source_type}: {e.detail}")
            continue

        # 2. 提取内容和文件名 (根据 source_type)
        content = None
        filename = "inline_doc"  # 默认文件名

        if source_type == "text":
            content = req.text
            filename = "inline_text"

        elif source_type == "uri":
            content = req.uri
            # 尝试从 URI 提取文件名
            if content:
                filename = content.split("/")[-1].split("?")[0].split("#")[0] or "remote_file"

        elif source_type == "base64":
            # Base64Source 期望一个 base64 字符串
            content = req.base64_content
            filename = "base64_input"

        # 3. 构造 Runner (source_type 匹配)
        logger.info(f"source_teem:{req.source_system}")
        runner = _make_runner(
            filename=filename,
            content=content,
            metadata=req.metadata,
            embedding_client=embedding_client,
            llm_client=llm_client,
            source_type=source_type,
            source_system=req.source_system # <-- PASSED
        )

        # 4. ThreadPool 运行 pipeline（同步接口，通过线程池 offload 阻塞任务）
        try:
            # 使用 run_in_threadpool 确保同步的 runner.run() 不会阻塞主 async 线程
            result = await run_in_threadpool(runner.run)
            all_results.append({"status": "ok", "source_type": source_type, "result": result})

        except Exception as e:
            logger.error(f"Structured ingest pipeline failed for {source_type}: {e}")
            all_results.append({"status": "failed", "source_type": source_type, "error": f"Pipeline failed: {e}"})

    # 返回批量处理的结果
    return {"status": "completed", "total_requests": len(requests), "results": all_results}