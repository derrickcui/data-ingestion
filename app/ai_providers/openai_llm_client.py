# app/ai_providers/openai_llm_client.py
from openai import OpenAI
from app.utility.log import logger

class OpenAILLMClient:
    """
    封装 OpenAI Chat API，用于摘要、关键词提取、business glossary 等文本分析
    """

    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.client = OpenAI(api_key=api_key)
        self.model = model
        #logger.info(f"OpenAI LLM initialized with model: {model}")

    def analyze(self, text: str, task: str = "business_glossary") -> str:
        if task == "summary":
            prompt = f"请为下面文本生成摘要：\n{text}"
        elif task == "keywords":
            prompt = f"请从下面文本提取关键词，JSON 数组输出：\n{text}"
        elif task == "business_glossary":
            prompt = f"抽取 business glossary 并输出 JSON：\n{text}"
        else:
            prompt = f"请执行任务 {task}：\n{text}"

        resp = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}]
        )
        return resp.choices[0].message.content if resp.choices else ""
