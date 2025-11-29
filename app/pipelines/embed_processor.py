from typing import Dict, Any, Optional, Callable
from app.pipelines.base import BaseProcessor
from app.utility.log import logger

class EmbedProcessor(BaseProcessor):
    """
    动态 Embedding 处理器。
    特点：
    1. 支持任何大模型提供商，只需在初始化时传入 embed_func。
    2. 对 data['chunks'] 中的每个文本块生成 embedding。
    3. 失败安全：调用异常会抛出，pipeline 停止。
    """

    order = 40

    def __init__(self, embed_func: Optional[Callable[[str], list]] = None, model_name: str = "custom"):
        """
        :param embed_func: 接收 str，返回 list[float] 的函数。必填。
        :param model_name: 日志显示用，标明使用的模型。
        """
        if embed_func is None:
            raise ValueError("EmbedProcessor requires embed_func parameter")
        self.embed_func = embed_func
        self.model_name = model_name

    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        chunks = data.get("chunks", [])
        if not chunks:
            logger.warning("EmbedProcessor: no chunks to embed")
            return {"embeddings": []}

        embeddings = []
        for i, chunk in enumerate(chunks):
            try:
                vec = self.embed_func(chunk)
                embeddings.append({"text": chunk, "embedding": vec})
                logger.info(f"EmbedProcessor [{self.model_name}] processed chunk {i+1}/{len(chunks)}")
            except Exception as e:
                logger.error(f"EmbedProcessor [{self.model_name}] failed on chunk {i+1}: {e}", exc_info=True)
                raise RuntimeError(f"EmbedProcessor failed on chunk {i+1}") from e

        return {"embeddings": embeddings}
