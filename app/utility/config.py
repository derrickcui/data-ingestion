from typing import Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """
    Application configuration.
    """
    APP_NAME: Optional[str] = None
    VERSION: Optional[str] = None
    DEBUG: Optional[bool] = None

    SEARCH_API_URL: Optional[str] = None
    INDEX_API_URL: Optional[str] = None
    GEELINK_API_KEY: Optional[str] = None
    LOAD_DOC_API_URL: Optional[str] = None

    OPENAI_API_KEY: Optional[str] = None
    OPENAI_MODEL: Optional[str] = "gpt-3.5-turbo"  # Default value
    OPENAI_EMBEDDING_MODEL: Optional[str] = None
    ALI_QWEN_API_KEY: Optional[str] = None
    ALI_QWEN_API_URL: Optional[str] = None
    ALI_QWEN_MODEL: Optional[str] = "qwen-plus"  # Default value
    ALI_EMBEDDING_MODEL: Optional[str] = None
    ALI_DS_MODEL: Optional[str] = None
    ALI_QWEN_MAX_MODEL: Optional[str] = None

    OLLAMA_URL: Optional[str] = "http://localhost:11434"  # Default value
    ALLOWED_ORIGINS: Optional[str] = "http://localhost:3000"  # Default value

    GOOGLE_API_KEY: Optional[str] = None

    CHROMA_SERVER_HOST: Optional[str] = "101.201.59.188"
    CHROMA_SERVER_HTTP_PORT: Optional[int] = 8000
    CHROMA_COLLECTION_NAME: Optional[str] = None
    SOLR_URL: Optional[str] = None
    TIKA_SERVICE_URL: Optional[str] = None
    TIKA_SERVICE_TIMEOUT: Optional[int] = 8000
    TESSERACT_PATH: Optional[str] = None
    LIBRE_OFFICE_PATH: Optional[str] = None
    POPPLER_PATH: Optional[str] = None
    LOCAL_MODEL_PATH: Optional[str] = None
    DEFAULT_PROMOTE_TEMPLATE: Optional[str] = "Use the following pieces of context to answer the question at the end. If you don't know the answer, just say that you don't know, don't try to make up an answer."
    DEFAULT_LLM_PROMOTE_TEMPLATE: Optional[str] = """
                      【指令】 
                      请你严格遵循以下回答原则：
                      1. 仅基于我提供的上下文信息回答问题
                      2. 如果问题超出提供的内容范围，必须回答"根据已知信息无法回答该问题"
                      3. 禁止推测、假设或添加任何非提供内容的信息
                      4. 当答案不确定时保持谨慎，宁可拒绝回答也不编造
                  
                      【当前对话规则】
                      - 回答长度限制：不超过200字
                      - 回答语言：与提问语言保持一致
                      - 禁用功能：互联网搜索/外部知识调用
                      - 引用格式：如需要引用，使用「引号」标注原文
                  
                      【违规处理】
                      如发现违反上述规则，将立即终止对话
                      """
    DEFAULT_TEMPERATURE: Optional[float] = 0.7
    DEFAULT_ROW_OF_DOCS: Optional[int] = 5
    PYTHON_PATH: Optional[str] = None
    UNOCONV_PATH: Optional[str] = None
    # ⭐⭐⭐ 新增：Redis + Celery 配置 ⭐⭐⭐
    REDIS_BROKER_URL: Optional[str] = None  # 示例：redis://localhost:6379/0
    REDIS_BACKEND_URL: Optional[str] = None  # 示例：redis://localhost:6379/1

    TIKA_URL: Optional[str] = "http://localhost:9998"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

Config = Settings()