from typing import Dict, Any, Optional
from app.pipelines.base import BaseProcessor
import re
import unicodedata
import string
from app.utility.log import logger


class CleanProcessor(BaseProcessor):
    """
    数据清洗处理器。
    优先使用 raw_text 避免中文乱码，同时保留 original_text。
    """


    order = 20


    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        # 优先使用 TikaProcessor 输出的 raw_text
        raw_text = data.get("raw_text")
        binary = data.get("binary")

        if raw_text:
            original_text = str(raw_text)
        elif binary:
            # 只有没有 raw_text 时才解码 binary
            encodings = ["utf-8", "utf-16", "gbk", "latin1"]
            for enc in encodings:
                try:
                    original_text = binary.decode(enc)
                    break
                except Exception:
                    continue
            else:
                original_text = binary.decode("utf-8", errors="replace")
        else:
            original_text = ""

        text = original_text

        # 1. Unicode 标准化
        text = unicodedata.normalize("NFKC", text)
        text = text.replace("\x00", "")
        text = text.replace("\r\n", "\n").replace("\r", "\n")

        # 2. 过滤特殊符号
        allowed_pattern = rf"[^\w\s\u4e00-\u9fa5\u3000-\u303f\uff00-\uffef{re.escape(string.punctuation)}]"
        text = re.sub(allowed_pattern, "", text)

        # 3. 空白和换行清理
        text = re.sub(r"[^\S\r\n]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)

        # 4. 移除不可打印字符
        text = "".join(ch for ch in text if ch.isprintable() or ch in ["\n", "\t"])
        text = text.strip()

        logger.info(f"CleanProcessor output preview: {text[:200]}{'...' if len(text) > 200 else ''}")

        return {
            "original_text": original_text,
            "raw_text": text,
            "clean_text": text
        }