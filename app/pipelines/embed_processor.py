from app.ai_providers.aliyun_client import AliEmbeddingClient
from app.ai_providers.google_client import GoogleEmbeddingClient
from app.ai_providers.openai_client import OpenAIEmbeddingClient
from app.pipelines.base import BaseProcessor
from typing import Dict, Any, Optional
from app.utility.config import Config

class EmbedProcessor(BaseProcessor):
    """
    统一 Embedding 处理器，支持 OpenAI / 阿里 / Google。
    """

    order = 40

    def __init__(self, client=None, model: str = None):
        """
        :param client: 任意封装好的 Embedding Client 实例
        :param model: 可选模型名称
        """
        self.client = client
        if model is not None:
            self.model = model
        elif client is not None:
            # 根据 client 类型选择默认模型
            if isinstance(client, OpenAIEmbeddingClient):
                self.model = Config.OPENAI_EMBEDDING_MODEL
            elif isinstance(client, AliEmbeddingClient):
                self.model = Config.ALI_EMBEDDING_MODEL
            elif isinstance(client, GoogleEmbeddingClient):
                self.model = Config.GOOGLE_EMBEDDING_MODEL
            else: self.model = None
        else:
            self.model = None

    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        chunks = data.get("chunks", [])
        if not chunks or self.client is None:
            return {"embeddings": []}

        embeddings = []
        for chunk in chunks:
            vec = self.client.embed(chunk, model=self.model)
            embeddings.append({"text": chunk, "embedding": vec})

        return {"embeddings": embeddings}
