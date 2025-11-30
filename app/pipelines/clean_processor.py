# app/pipelines/clean_processor.py
from app.pipelines.base import BaseProcessor
from typing import Dict, Any, Optional
import re
import unicodedata
import ftfy
from bs4 import BeautifulSoup
from app.utility.log import logger

# ============= 可开关的高级能力 =============
ENABLE_HTML_PARSE = True          # 是否启用 HTML → Markdown 还原（Tika 输出 HTML 时必须开）
ENABLE_SEMANTIC_DEDUP = True      # 改成 True！我们已经下载好本地模型了
ENABLE_COMPLIANCE_MASK = True     # 是否启用企业合规脱敏（手机号、身份证、邮箱、微信号等）

# 语义去重模型（仅在启用时才加载）
dedup_model = None

if ENABLE_SEMANTIC_DEDUP:
    try:
        import os
        from sentence_transformers import SentenceTransformer
        from app.utility.log import logger

        # 【关键修改】指向你实际下载的本地路径（Git Bash / Windows 都支持）
        LOCAL_MODEL_PATH = "c:/work/model/models/bge-small-zh-v1.5"
        # 如果你更喜欢写成 Windows 风格也可以（二选一就行）：
        # LOCAL_MODEL_PATH = r"C:\work\model\models\bge-small-zh-v1.5"

        if not os.path.exists(LOCAL_MODEL_PATH):
            raise FileNotFoundError(f"模型目录不存在: {LOCAL_MODEL_PATH}")

        dedup_model = SentenceTransformer(
            LOCAL_MODEL_PATH,
            device="cpu",              # 有独立显卡改成 "cuda"
            local_files_only=True,     # 强制只用本地文件，永不联网
        )
        logger.info("bge-small-zh-v1.5 本地模型加载成功（路径: /c/work/model/models/bge-small-zh-v1.5）")
    except Exception as e:
        logger.warning(f"bge-small-zh-v1.5 加载失败，自动关闭语义去重功能: {e}")
        ENABLE_SEMANTIC_DEDUP = False

class CleanProcessor(BaseProcessor):
    """
    终极企业级清洗处理器（融合了你原有全部优秀逻辑 + Derrick 的 UltimateCleaner 全套能力）
    """
    order = 20

    # ==================== 企业级黑名单 ====================
    BLACKLIST_PATTERNS = [
        r"版权所有.*?\d{4}", r"未经许可.{0,10}不得转载", r"未经书面.{0,10}授权",
        r"微信.?号[:：]?\s*[\w\-]{5,}", r"公众号[:：].{5,}", r"招聘.{0,20}\d{3,}",
        r"电话[:：]?\d{7,}", r"地址[:：]?.{5,}路.{0,10}号",
        r"第\s*\d+\s*页\s*(?:/\s*共\s*\d+\s*页)?", r"机密.{0,10}保密级别", r"内部资料.{0,10}仅限内部",
        r"Confidential\s*$", r"Internal\s+Use\s+Only\s*$", r"Restricted\s*$",
    ]
    COMPILED_BLACKLIST = [re.compile(p, re.I) for p in BLACKLIST_PATTERNS]

    def _l1_encoding_fix(self, text: str) -> str:
        """L1 终极编码修复（ftfy + NFC + utf-8 ignore）"""
        text = ftfy.fix_text(text, normalization='NFC')
        text = unicodedata.normalize("NFC", text)
        text = text.encode('utf-8', 'ignore').decode('utf-8')
        return text

    def _l2_layout_noise_removal(self, text: str) -> str:
        lines = text.split('\n')
        cleaned = []

        for line in lines:
            l = line.strip()
            if not l:
                cleaned.append(line)
                continue

            if (re.match(r"^第?\s*\d+\s*页", l) or
                    re.match(r"^\d+\s*/\s*\d+$", l) or
                    re.match(r"^[-─━—~～\.·_ ]{8,}\s*\d*\s*$", l) or
                    re.match(r"^（?(机密|秘密|内部|保密).*?）?\s*$", l, re.I) or
                    re.fullmatch(r"\s*\d+\s*", l) and len(l.strip()) <= 10 or
                    l in ["页", "第 页", "第页"]):
                continue

            cleaned.append(line)

        text = '\n'.join(cleaned)

        # ================== 终极补丁：专杀“行尾逗号 + 下一行顶格” ==================
        # 情况1：行尾是逗号，下一行顶格是汉字 → 直接吃掉换行 + 加一个空格
        text = re.sub(r",\s*\n+\s*([\u4e00-\u9fa5])", r", \1", text)

        # 情况2：行尾是顿号，下一行顶格是汉字 → 同上（公文超爱用顿号）
        text = re.sub(r"、\s*\n+\s*([\u4e00-\u9fa5])", r"、\1", text)

        # 跨页页码横线终极绝杀（最先执行！）
        text = re.sub(r"[—–\-─━—]+\s*\n+\s*\d+", "", text)  # —\n1
        text = re.sub(r"\d+\s*[—–\-─━—]+", "", text)  # 2 —
        text = re.sub(r"^\s*[—–\-─━—]+\s*$", "", text, flags=re.M)
        text = re.sub(r"^\s*\d+\s*$", "", text, flags=re.M)

        # 原有终极清洗
        text = re.sub(r"[\uFFFC\uFFFD\u200B-\u200F\u2060-\u206F\uFEFF\ufff0-\uffff]", "", text)
        text = re.sub(r"^\s*[。．.,，、]\s*$", "", text, flags=re.M)

        # 正常断行修复
        text = re.sub(r"([。！？；])\s*\n+\s*", r"\1\n\n", text)
        text = re.sub(r"([，、：；”’）】])\s*\n+\s*", r"\1 ", text)
        text = re.sub(r"([\u4e00-\u9fa5])\n+([\u4e00-\u9fa5])", r"\1\2", text)
        text = re.sub(r"(\w+)[-─━—~～]\s*\n\s*(\w+)", r"\1\2", text)
        text = re.sub(r"(\w+)\s*[-─━—~～]\s+(\w+)", r"\1\2", text)

        return text

    def _l3_html_structure_restore(self, html: str) -> str:
        """L3 HTML → 干净 Markdown（仅在 Tika 输出 HTML 时使用）"""
        soup = BeautifulSoup(html, "html.parser")
        # 删除垃圾标签
        for tag in soup(["script", "style", "header", "footer", "nav", "aside"]):
            tag.decompose()

        # 表格 → Markdown
        for table in soup.find_all("table"):
            rows = []
            for tr in table.find_all("tr"):
                cells = [c.get_text(strip=True).replace("\n", " ") for c in tr.find_all(["td", "th"])]
                rows.append("| " + " | ".join(cells) + " |")
            if rows:
                sep = "| " + " | ".join(["---" for _ in cells]) + " |"
                rows.insert(1, sep)
                table.replace_with("\n".join(rows) + "\n\n")

        # 标题转 Markdown
        for h in soup.find_all(re.compile("^h[1-6]$")):
            level = int(h.name[1])
            h.insert_before("#" * level + " " + h.get_text().strip() + "\n\n")
            h.decompose()

        return soup.get_text(separator="\n")

    def _l5_compliance_and_pii(self, text: str) -> str:
        """L5 企业合规 + PII 脱敏"""
        if not ENABLE_COMPLIANCE_MASK:
            return text

        # 业务黑名单整行/整段删除
        for pat in self.COMPILED_BLACKLIST:
            text = pat.sub("", text)

        # 手机号脱敏（保留前3后4）
        text = re.sub(r"1[3-9]\d{9}", lambda m: m.group()[:3] + "****" + m.group()[-4:], text)
        # 身份证号脱敏
        text = re.sub(r"[1-6]\d{5}(18|19|20)\d{2}\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d{3}[\dXx]",
                      lambda m: m.group()[:6] + "********" + m.group()[-4:], text)
        # 邮箱脱敏（可选）
        # text = re.sub(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", "****@****.com", text)

        return text

    def _semantic_dedup(self, paragraphs: list[str], threshold: float = 0.94) -> list[str]:
        """可选的段落级语义去重（对 PPT、重复页脚的 PDF 极有效）"""
        if not ENABLE_SEMANTIC_DEDUP or dedup_model is None or len(paragraphs) <= 1:
            return paragraphs

        import numpy as np
        embeddings = dedup_model.encode(paragraphs, normalize_embeddings=True, batch_size=32, show_progress_bar=False)
        keep = [0]
        for i in range(1, len(paragraphs)):
            sims = np.dot(embeddings[i], embeddings[keep].T)
            if sims.max() < threshold:
                keep.append(i)
        return [paragraphs[i] for i in keep]

    def process(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        # ============ 1. 获取原始文本 ============
        raw_text = data.get("raw_text")
        binary = data.get("binary")
        is_tika_html = data.get("source", "").lower() == "tika" and "<html" in str(raw_text)[:500]

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

        # ============ L1 编码终极修复 ============
        text = self._l1_encoding_fix(text)

        # ============ L3 HTML 结构还原（仅 Tika HTML） ============
        if ENABLE_HTML_PARSE and is_tika_html:
            try:
                text = self._l3_html_structure_restore(text)
            except Exception as e:
                logger.warning(f"HTML 解析失败，回退纯文本处理: {e}")

        # ============ 你原有优秀逻辑（保留+增强） ============
        text = unicodedata.normalize("NFKC", text)
        text = text.replace('\x00', '')
        text = text.replace("\r\n", "\n").replace("\r", "\n")

        # ============ L2 布局噪声精准删除 ============
        text = self._l2_layout_noise_removal(text)

        # ============ 行级清理（你原有逻辑） ============
        processed_lines = []
        for line in text.split('\n'):
            line = re.sub(r'[\s\u00A0\u200B\u3000]+', ' ', line)
            line = line.strip()
            if not line:
                processed_lines.append('')
                continue

            if re.fullmatch(r'[\d\W\s]+', line) and len(line) < 10:
                continue
            line = "".join(ch for ch in line if ch.isprintable())
            line = re.sub(r'\[\w+:\s*\w+\]', '', line).strip()

            if line:
                processed_lines.append(line)
            else:
                processed_lines.append('')

        # ============ 智能段落合并（你原有极佳逻辑） ============
        paragraphs = []
        current_paragraph = []

        cn_terminal = r'[。！？;；:：”’)]'

        for line in processed_lines:
            if not line:
                if current_paragraph:
                    paragraphs.append(' '.join(current_paragraph))
                    current_paragraph = []
                paragraphs.append('')   # 保留空行作为段落分隔
            else:
                if current_paragraph and re.search(rf'{cn_terminal}\s*$', current_paragraph[-1]):
                    current_paragraph.append(line)
                    paragraphs.append(' '.join(current_paragraph))
                    current_paragraph = []
                else:
                    current_paragraph.append(line)

        if current_paragraph:
            paragraphs.append(' '.join(current_paragraph))

        # 清理空段落
        paragraphs = [p for p in paragraphs if p.strip() or p == '']

        # ============ 可选语义去重 ============
        non_empty_paras = [p for p in paragraphs if p.strip()]
        if non_empty_paras:
            deduped = self._semantic_dedup(non_empty_paras)
            # 重新插回空行位置（简单策略：均匀分布或全部保留原空行）
            # 这里直接用去重后的非空段落 + 原空行保留
            paragraphs = deduped + [''] * (len(paragraphs) - len(non_empty_paras))

        # ============ 重新拼接 ============
        text = '\n\n'.join(p.strip() for p in paragraphs if p.strip()).strip()

        # ============ L5 合规脱敏 ============
        text = self._l5_compliance_and_pii(text)

        # ============ 最终统一格式化 ============
        text = re.sub(r'([\u4e00-\u9fa5])[\s\u00A0\u200B\u3000]*([\u4e00-\u9fa5])', r'\1\2', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r' *\n\n *', '\n\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = text.strip()

        final_text = text if len(text) > 30 else ""

        return {
            "clean_text": final_text
        }