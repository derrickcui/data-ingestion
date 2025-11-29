import requests
from typing import Dict, Any, Optional
from app.pipelines.base import BaseProcessor
from app.utility.log import logger


class TikaProcessor(BaseProcessor):
    """
    使用 Tika Server 抽取文本。
    接口规范：
        输入： data["binary"] (bytes)
        输出： {"raw_text": "..."}  —— 不修改原始 data
    """
    order = 10  # 放在比较早的阶段（解析 → 清洗 → chunk）

    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        logger.info("开始处理文件")
        binary = data.get("binary")
        if not binary:
            logger.warning("TikaProcessor: no binary data found.")
            return {"raw_text": ""}

        try:
            r = requests.put(
                "http://localhost:9998/tika",
                data=binary,
                # Tika Server 通常会返回 Content-Type: text/plain; charset=UTF-8，
                # 但如果它没有返回，我们最好强制指定编码。
                headers={"Accept": "text/plain"}
            )
            r.raise_for_status()

            # --- 核心修改：强制 requests 使用 UTF-8 解码 ---
            # 1. 尝试从响应头获取编码，如果失败，则强制设置为 'utf-8'。
            # 2. 如果 r.encoding 已经被正确识别，则使用它。
            #    如果 r.encoding 是 None 或 requests 猜测错误（如 'ISO-8859-1'），
            #    则显式将其设置为 'utf-8'，因为 Tika 默认使用 UTF-8 输出。
            if r.encoding is None or r.encoding.lower() != 'utf-8':
                # 假设 Tika Server 返回的文本是 UTF-8 编码的
                r.encoding = 'utf-8'

            text = r.text or ""
            # --- 核心修改结束 ---

            logger.info(f"TikaProcessor extracted text length={len(text)}")

            # 为了调试和避免日志文件本身乱码，我们只打印文本的片段
            log_preview = text[:200] + ('...' if len(text) > 200 else '')
            logger.info(f"TikaProcessor preview: {log_preview}")

            return {"raw_text": text}

        except Exception as e:
            logger.error(f"TikaProcessor failed: {e}")
            raise