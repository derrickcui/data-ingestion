# app/ai_providers/google_client.py
class GoogleEmbeddingClient:
    """
    封装 Google Embeddings API，统一接口：embed(text, model)
    """

    def __init__(self, api_key: str):
        self.api_key = api_key
        # 初始化 Google AI SDK

    def embed(self, text: str, model: str) -> list[float]:
        # 调用 Google embedding API
        return [0.5, 0.6, 0.7]
