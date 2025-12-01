from typing import Dict, Any, Optional
from app.pipelines.base import BaseProcessor

class LLMProcessor(BaseProcessor):
    """
    基于 OpenAI Chat API 的处理器，用于从 clean_text 中抽取 business glossary。
    """

    order = 50  # pipeline 执行顺序，可调整

    def __init__(self, client, task: str = "business_glossary"):
        self.client = client
        self.task = task

    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        text = data.get("clean_text", "")
        if not text:
            return {"business_glossary": ""}

        result = self.client.analyze(text, self.task)
        return {"business_glossary": result}

    """
    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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

    """