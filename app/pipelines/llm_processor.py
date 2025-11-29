from typing import Dict, Any, Optional
from app.pipelines.base import BaseProcessor
from openai import OpenAI

class LLMProcessor(BaseProcessor):
    """
    基于 OpenAI Chat API 的处理器，用于从 clean_text 中抽取 business glossary。
    """

    order = 50  # pipeline 执行顺序，可调整

    def __init__(self, api_key: str, model: str = "gpt-4o-mini", max_tokens: int = 4000):
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.max_tokens = max_tokens

    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        对 data["clean_text"] 的前 max_tokens 字生成 business glossary JSON。
        """
        text = data.get("clean_text", "")
        if not text:
            return {"metadata": ""}

        truncated_text = text[:self.max_tokens]

        resp = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "user",
                    "content": f"抽取 business glossary 并输出 JSON: {truncated_text}"
                }
            ]
        )

        content = resp.choices[0].message.content if resp.choices else ""

        return {"metadata": content}

