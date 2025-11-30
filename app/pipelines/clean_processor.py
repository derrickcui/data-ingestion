from typing import Dict, Any, Optional
from app.pipelines.base import BaseProcessor
import re
import unicodedata
import string
from app.utility.log import logger

class CleanProcessor(BaseProcessor):
    """
    数据清洗处理器（最终版）。
    - 保留段落分隔符 \n\n
    - 删除行内多余空格、零宽空格、全角空格、NBSP等
    - 汉字之间不再出现残余空格
    - 清理页码、垃圾行和不可打印字符
    """

    order = 20

    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        raw_text = data.get("raw_text")
        binary = data.get("binary")

        if raw_text:
            original_text = str(raw_text)
        elif binary:
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

        # --- Unicode 标准化 ---
        text = unicodedata.normalize("NFKC", text)
        text = text.replace('\x00', '')

        # --- 标准化换行 ---
        text = text.replace("\r\n", "\n").replace("\r", "\n")

        # --- 行级清理 ---
        processed_lines = []
        for line in text.split('\n'):
            # 删除所有不可见空白字符
            line = re.sub(r'[\s\u00A0\u200B\u3000]+', ' ', line)
            line = line.strip()

            if not line:
                continue

            # 删除页码或纯垃圾行
            if re.fullmatch(r'[\d\W\s]+', line) and len(line) < 10:
                continue

            # 删除不可打印字符
            line = "".join(ch for ch in line if ch.isprintable())
            # 删除行内 bookmark 或注释
            line = re.sub(r'\[\w+:\s*\w+\]', '', line).strip()

            if line:
                processed_lines.append(line)

        # --- 智能段落合并 ---
        text = ""
        cn_terminal_punctuation = r'[。！？;；:：”’)]'

        for i, current_line in enumerate(processed_lines):
            is_paragraph_break = False

            if i > 0:
                previous_line = processed_lines[i - 1]

                # 上一行以句末标点结束，则段落分隔
                if previous_line and re.search(rf'{cn_terminal_punctuation}\s*$', previous_line):
                    is_paragraph_break = True

                if is_paragraph_break:
                    text += '\n\n'
                else:
                    text += ' '

            text += current_line

        # --- 删除汉字之间残余空格 ---
        # 包含普通空格、全角空格、零宽空格、NBSP
        text = re.sub(r'([\u4e00-\u9fa5])[\s\u00A0\u200B\u3000]*([\u4e00-\u9fa5])', r'\1\2', text)

        # --- 删除多余空格，保留段落换行 ---
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r' *\n\n *', '\n\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = text.strip()

        return {
            #"original_text": original_text,
            "raw_text": original_text,
            "clean_text": text
        }
