from typing import Dict, Any, Optional
from app.pipelines.base import BaseProcessor
from langchain_text_splitters import RecursiveCharacterTextSplitter


class ChunkProcessor(BaseProcessor):
    """
    使用 LangChain 进行智能文本切分的处理器。
    特点：
    1. 递归切分：优先按段落切，再按句子切，最后按字符切，保持语义完整。
    2. Overlap：块与块之间有重叠，防止上下文在边界丢失。
    """

    order = 30

    def __init__(self, chunk_size: int = 500, chunk_overlap: int = 50):
        """
        :param chunk_size: 每个块的目标字符数（注意：是字符数，不是词数）
        :param chunk_overlap: 相邻块之间的重叠字符数（关键参数，通常设为 10%-20%）
        """
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

        # 初始化 LangChain 切分器
        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap,
            # 分隔符优先级：双换行(段落) > 单换行(句子) > 空格 > 空字符
            separators=["\n\n", "\n", " ", ""],
            length_function=len,
            is_separator_regex=False,
        )

    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        text = data.get("clean_text", "")

        if not text:
            return {"chunks": []}

        # 使用 LangChain 的 split_text 方法
        # 它返回的是字符串列表
        chunks = self.splitter.split_text(text)

        # 这里我们也可以顺便统计一下元数据，比如切分后的块数量
        print(f"Split text into {len(chunks)} chunks.")

        return {"chunks": chunks}