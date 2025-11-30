# app/ai_providers/openai_client.py

from openai import OpenAI
from app.utility.log import logger

class OpenAIEmbeddingClient:
    """
    封装 OpenAI Embeddings API，统一接口：embed(text, model)
    """
    def __init__(self, api_key: str):
        self.client = OpenAI(api_key=api_key)
        #logger.info(f"open key:{api_key}")

    def embed(self, text: str, model: str = "text-embedding-3-small") -> list[float]:
        """
        生成文本向量
        :param text: 文本字符串
        :param model: embedding 模型名称
        :return: 向量列表
        """
        #logger.info(f"OpenAI embedding model: {model}")
        response = self.client.embeddings.create(model=model, input=text)
        return response.data[0].embedding
