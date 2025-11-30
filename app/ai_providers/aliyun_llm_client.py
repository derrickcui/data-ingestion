# app/ai_providers/aliyun_llm_client.py

import json
from dashscope import Generation
from app.utility.log import logger


class AliyunLLMClient:
    """
    阿里云通义千问大模型封装
    与 OpenAI LLM Client 保持一致接口：
        analyze(text, task)
    """

    def __init__(self, api_key: str, model: str = "qwen-plus"):
        self.api_key = api_key
        self.model = model
        logger.info(f"Aliyun LLM initialized with model: {model}")

    # =============================
    #   核心：统一任务意图 -> Prompt
    # =============================
    def _build_prompt(self, text: str, task: str) -> str:
        if task == "summary":
            return f"""
                    你是一个中文文档摘要助手。
                    请对下面的文本生成简洁、准确的摘要：
                    
                    {text}
                    """

        elif task == "keywords":
            return f"""
                    请从下面的文本中抽取 3~10 个关键词，并用 JSON 数组返回，例如 ["关键词A", "关键词B"]：
                    
                    {text}
                    """

        elif task == "business_glossary":
            return f"""
                    你是一个企业术语抽取专家。
                    请从下面文本中抽取“业务术语”：要求格式为 JSON，例如：
                    {{
                       "供应链": "企业中用于管理货物流转的系统",
                       "库存周转率": "衡量库存效率的财务指标"
                    }}

                    下面是文本：
                    
                    {text}
                    """

        # 默认任务
        return f"请执行任务 `{task}`，对以下文本进行分析：\n\n{text}"

    # =============================
    #   主方法：analyze(text, task)
    # =============================
    def analyze(self, text: str, task: str = "business_glossary") -> str:
        prompt = self._build_prompt(text, task)
        logger.info(f"[Aliyun LLM] Task: {task}")

        try:
            response = Generation.call(
                model=self.model,
                prompt=prompt,
                api_key=self.api_key,
                result_format="message"
            )

            # Message 模式：response["output"]["text"]
            result = response["output"]["text"]
            #logger.info(f"[Aliyun LLM] Response: {result}")

            return result

        except Exception as e:
            logger.error(f"Aliyun LLM failed: {e}")
            raise RuntimeError(f"Aliyun LLM request error: {e}")
