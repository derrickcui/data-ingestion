# app/api/routes/email_ingest.py
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from fastapi.concurrency import run_in_threadpool
from concurrent.futures import ThreadPoolExecutor
from app.orchestrator.pipeline_runner import PipelineRunner
from app.sources.email_source import EmailSource
from app.sinks.solr_sink import SolrSink
from app.pipelines.processor_registry import load_all_processor_classes
from app.ai_providers.aliyun_client import AliEmbeddingClient
from app.ai_providers.openai_client import OpenAIEmbeddingClient
from app.ai_providers.google_client import GoogleEmbeddingClient
from app.ai_providers.aliyun_llm_client import AliyunLLMClient
from app.ai_providers.openai_llm_client import OpenAILLMClient
from app.ai_providers.google_llm_client import GoogleLLMClient
from app.utility.config import Config
from app.utility.log import logger

router = APIRouter()
executor = ThreadPoolExecutor(max_workers=20)  # 限制并发

# ---------------------------
# 请求模型
# ---------------------------
class EmailIngestRequest(BaseModel):
    host: str = Field(..., description="IMAP 服务器地址")
    port: int = Field(default=993, description="IMAP 端口")
    username: str = Field(..., description="邮箱账号")
    password: str = Field(..., description="邮箱密码")
    mailbox: Optional[str] = Field("INBOX", description="邮箱文件夹")
    max_emails: Optional[int] = Field(50, description="最大拉取邮件数量")
    provider: Optional[str] = Field(None, description="embedding/LLM provider，如 ali/openai/google")
    source_system: Optional[str] = Field(None, description="来源系统标识，用于 Doc ID 生成")
    metadata: Optional[Dict[str, Any]] = Field(None, description="用户自定义元数据")


# ---------------------------
# 初始化客户端
# ---------------------------
def _initialize_clients(provider: Optional[str]):
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


# ---------------------------
# 构建 Pipeline Runner
# ---------------------------
def _make_runner(email_source: EmailSource, embedding_client=None, llm_client=None):
    sinks = [SolrSink(Config.SOLR_URL, Config.SOLR_COLLECTION)]
    processor_classes = load_all_processor_classes()
    processors = []
    for cls in processor_classes:
        name = cls.__name__
        if name == "EmbedProcessor" and embedding_client:
            processors.append(cls(client=embedding_client))
        elif name == "LLMProcessor" and llm_client:
            processors.append(cls(client=llm_client))
        else:
            processors.append(cls())
    return PipelineRunner(email_source, processors, sinks)


"""
全量：
{
  "host": "imap.exmail.qq.com",
  "port": 993,
  "username": "derrick.cui@geelink.cn",
  "password": "!Aydc3588",
  "mailbox": "INBOX",
  "max_emails": 50,
  "provider": "ali",
  "reset_state": true
}
增量：
{
  "host": "imap.exmail.qq.com",
  "port": 993,
  "username": "your_email@qq.com",
  "password": "your_password",
  "mailbox": "INBOX",
  "max_emails": 50,
  "reset_state": false
}
"""
# ---------------------------
# Email Ingest API
# ---------------------------
@router.post("/ingest_email", summary="从邮箱抓取邮件并入库")
async def ingest_email(req: EmailIngestRequest):
    try:
        embedding_client, llm_client = _initialize_clients(req.provider)
    except HTTPException as e:
        raise e

    # 初始化 EmailSource
    email_source = EmailSource(
        host=req.host,
        port=req.port,
        username=req.username,
        password=req.password,
        mailbox=req.mailbox,
        max_emails=req.max_emails,
        user_metadata=req.metadata,
        source_type="email",
    )

    runner = _make_runner(email_source, embedding_client, llm_client)

    try:
        result = await run_in_threadpool(runner.run)
        return {"status": "ok", "total_emails": len(result), "result": result}
    except Exception as e:
        logger.error(f"Email ingest failed: {e}")
        raise HTTPException(500, f"Email ingest failed: {e}")
