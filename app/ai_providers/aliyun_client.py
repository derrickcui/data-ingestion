# app/ai_providers/aliyun_client.py

from typing import List
from langchain_community.embeddings import DashScopeEmbeddings
from app.utility.log import logger


class AliEmbeddingClient:
    """
    阿里云通义千问 Embedding 封装
    接口与 OpenAIEmbeddingClient 完全一致：
        embed(text: str, model: str) -> List[float]
    """

    def __init__(self, api_key: str):
        self.api_key = api_key

        # 阿里云 embedding 默认模型
        self.default_model = "text-embedding-v4"

        logger.info(f"Aliyun embedding key loaded.")

        # NOTE: DashScopeEmbeddings 会自动读取 dashscope_api_key
        self._embedder_cache = {}

    def _get_embedder(self, model: str):
        """
        避免重复创建 DashScopeEmbeddings，提升性能。
        """
        if model not in self._embedder_cache:
            self._embedder_cache[model] = DashScopeEmbeddings(model=model, dashscope_api_key=self.api_key)
        return self._embedder_cache[model]

    def embed(self, text: str, model: str = None) -> List[float]:
        """
        主 embedding 方法，统一接口：
            AliEmbeddingClient().embed(text, model="xxx")

        :param text: 输入文本
        :param model: 阿里云 embedding 模型
        :return: 向量 list[float]
        """
        if not text or not text.strip():
            return []

        model = model or self.default_model
        #logger.info(f"Aliyun embedding model: {model}")

        try:
            embedder = self._get_embedder(model)
            vector = embedder.embed_query(text)
            return vector

        except Exception as e:
            raise RuntimeError(f"Aliyun embedding failed: {e}")
