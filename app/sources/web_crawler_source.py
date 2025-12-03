# app/sources/web_crawler_source.py
import asyncio
import os
import re
from typing import Dict, Any, Optional, List, Set
from urllib.parse import urljoin, urlparse, urldefrag

import aiohttp
import trafilatura
from bs4 import BeautifulSoup
from urllib import robotparser

from app.sources.base import BaseSource
from app.utility.log import logger

AD_TRACKING_PATTERNS = [
    "doubleclick", "googlesyndication", "google-analytics", "googletagmanager",
    "adservice", "adsystem", "adclick", "facebook.com", "facebook.net",
    "baidu.com", "analytics", "tracker", "tracking", "ads.", "ad.",
]

COMMON_PAGE_EXTENSIONS = [".html", ".htm", ".php", ".aspx", ""]  # empty => no extension


def _normalize_url(url: str) -> str:
    """标准化：去 fragment，去尾部斜杠（保留根 /），转换为 utf-8 safe"""
    url, _ = urldefrag(url)
    return url.rstrip("/")


def _is_ad_link(url: str) -> bool:
    lower = url.lower()
    for p in AD_TRACKING_PATTERNS:
        if p in lower:
            return True
    return False


def _get_base_domain(netloc: str) -> str:
    parts = netloc.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return netloc


class WebCrawlerSource(BaseSource):
    """
    异步 Web Crawler Source（aiohttp）:
    - start_url: 起始 URL
    - max_depth: 最大递归深度（从 0 开始）
    - allowed_extensions: 允许下载的资源后缀（小写，'.pdf' 等）。HTML 页面通常以 '' 或 .html 匹配
    - concurrency: aiohttp 并发限制
    - allow_subdomains: 是否允许抓取子域（True：允许同根域的任意子域；False：只允许 start_url 的主机）
    - restrict_to_path: 是否限定到起始 URL 的目录（True：只抓取以 start_url.path 开头的 URL；False：不限定目录）
    - respect_robots: 是否遵守 robots.txt（True：会读取并判断 allow）
    返回每个抓取到的条目为 dict:
    {
        "file_name": ...,
        "binary": ... (仅二进制),
        "raw_text": ... (仅 HTML 或已提取文本),
        "source_path": original_url,
        "source_type": "web",
        "user_metadata": self.user_metadata,
        "score": float  # 内容质量/优先级分数（越大越好）
    }
    """

    def __init__(
        self,
        start_url: str,
        max_depth: int = 2,
        allowed_extensions: Optional[List[str]] = None,
        concurrency: int = 6,
        allow_subdomains: bool = True,
        restrict_to_path: bool = False,
        respect_robots: bool = True,
    ):
        self.start_url = start_url.rstrip("/")
        self.max_depth = max_depth
        self.allowed_extensions = [e.lower() for e in (allowed_extensions or [
            ".html", ".htm", ".pdf", ".txt", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"
        ])]
        self.concurrency = max(1, int(concurrency))
        self.user_metadata = None
        self.source_type = "web"
        self.allow_subdomains = allow_subdomains
        self.restrict_to_path = restrict_to_path
        self.respect_robots = respect_robots

        # 内部状态（线程内）
        self.visited_urls: Set[str] = set()
        self.seen_urls: Set[str] = set()  # 去重（标准化）
        self.results: List[Dict[str, Any]] = []

        # 解析起始 URL 信息
        parsed = urlparse(self.start_url)
        self.start_netloc = parsed.netloc.lower()
        self.start_base_domain = _get_base_domain(self.start_netloc)
        self.start_path_prefix = parsed.path if parsed.path.endswith("/") else os.path.dirname(parsed.path) + "/"

        # robots parser（延迟加载）
        self._robots = None
        if self.respect_robots:
            try:
                rp = robotparser.RobotFileParser()
                robots_url = f"{parsed.scheme}://{self.start_netloc}/robots.txt"
                rp.set_url(robots_url)
                rp.read()
                self._robots = rp
            except Exception as e:
                logger.warning(f"[WebCrawlerSource] Failed to load robots.txt: {e}")
                self._robots = None

    # -------------------------
    # 外部同步接口（Pipeline 调用）
    # -------------------------
    def read(self, context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        同步入口：内部使用 asyncio.run() 执行异步爬取（在线程池中运行时是安全的）。
        """
        context = context or {}
        # reset per-run state
        self.visited_urls.clear()
        self.seen_urls.clear()
        self.results.clear()

        logger.info(f"[WebCrawlerSource] Async crawl starting: {self.start_url} (max_depth={self.max_depth})")
        try:
            # 在 thread 中调用 asyncio.run 是可行的（确保 PipelineRunner.run 在线程池内）
            asyncio.run(self._crawl_async())
        except RuntimeError as e:
            # 如果在已经有运行的事件循环中调用（极少数情形），fallback 使用 new_event_loop
            logger.debug(f"[WebCrawlerSource] asyncio.run failed (maybe already running): {e}. using new loop.")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._crawl_async())
            loop.close()

        logger.info(f"[WebCrawlerSource] Crawl finished: found {len(self.results)} items")
        # 注入 user_metadata 到每个结果（如果存在）
        if self.user_metadata:
            for r in self.results:
                r.setdefault("user_metadata", {}).update(self.user_metadata)
        return self.results

    # -------------------------
    # 异步核心爬取逻辑
    # -------------------------
    async def _crawl_async(self):
        connector = aiohttp.TCPConnector(ssl=False)  # 根据需要调整
        timeout = aiohttp.ClientTimeout(total=30)
        sem = asyncio.Semaphore(self.concurrency)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # 使用队列进行 BFS 风格抓取（更易控制深度）
            queue = asyncio.Queue()
            start_url_norm = _normalize_url(self.start_url)
            await queue.put((start_url_norm, 0))

            workers = [asyncio.create_task(self._worker(queue, session, sem)) for _ in range(self.concurrency)]
            await queue.join()
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    async def _worker(self, queue: asyncio.Queue, session: aiohttp.ClientSession, sem: asyncio.Semaphore):
        while True:
            try:
                url, depth = await queue.get()
            except asyncio.CancelledError:
                return
            try:
                if url in self.visited_urls:
                    continue
                # 1) robots.txt 检查
                if self._is_disallowed_by_robots(url):
                    logger.debug(f"[WebCrawlerSource] Disallowed by robots.txt: {url}")
                    self.visited_urls.add(url)
                    continue
                # 2) 广告/追踪过滤
                if _is_ad_link(url):
                    logger.debug(f"[WebCrawlerSource] Skipping ad/tracking link: {url}")
                    self.visited_urls.add(url)
                    continue
                # 3) 同域名 / 子域 / 同目录 检查
                if not self._is_url_in_scope(url):
                    logger.debug(f"[WebCrawlerSource] Out-of-scope URL: {url}")
                    self.visited_urls.add(url)
                    continue

                # 标记访问（防重复排队）
                self.visited_urls.add(url)

                async with sem:
                    await self._fetch_and_process(url, depth, queue, session)
            except Exception as e:
                logger.error(f"[WebCrawlerSource] Worker exception for {url}: {e}", exc_info=True)
            finally:
                try:
                    queue.task_done()
                except Exception:
                    pass

    def _is_disallowed_by_robots(self, url: str) -> bool:
        if not self._robots:
            return False
        try:
            return not self._robots.can_fetch("*", url)
        except Exception:
            return False

    def _is_url_in_scope(self, url: str) -> bool:
        """
        判断是否在允许的抓取范围：
        - host 必须与 start_netloc 相同，或者以 start_base_domain 结尾（如果 allow_subdomains True）
        - 如果 restrict_to_path True，则 path 必须以 start_path_prefix 开头（限定目录）
        """
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        path = parsed.path or "/"

        # 主机检查
        if netloc == self.start_netloc:
            pass
        elif self.allow_subdomains and netloc.endswith("." + self.start_base_domain):
            pass
        else:
            return False

        # 限定目录检查
        if self.restrict_to_path:
            # 例如 start_path_prefix = '/zt_d/subject-1744249144/'
            if not path.startswith(self.start_path_prefix):
                return False

        return True

    async def _fetch_and_process(self, url: str, depth: int, queue: asyncio.Queue, session: aiohttp.ClientSession):
        logger.info(f"[WebCrawlerSource] Fetching ({depth}): {url}")
        try:
            async with session.get(url, allow_redirects=True) as resp:
                status = resp.status
                if status != 200:
                    logger.debug(f"[WebCrawlerSource] Non-200 status {status} for {url}")
                    return

                content_type = resp.headers.get("Content-Type", "").lower()
                content = await resp.read()

                # 判断资源类型（优先做扩展名判断）
                parsed = urlparse(url)
                file_name = os.path.basename(parsed.path) or "index.html"
                ext = os.path.splitext(file_name)[1].lower()

                # HTML 页面
                if "text/html" in content_type or ext in COMMON_PAGE_EXTENSIONS:
                    text = None
                    try:
                        # 优先用 trafilatura 提取正文（比简单 soup 更稳健）
                        # trafilatura.extract 接受 bytes 或 str
                        text = trafilatura.extract(content.decode(resp.get_encoding() or "utf-8", errors="ignore"), include_comments=False, favor_precision=True)
                        if not text:
                            # fallback to BeautifulSoup
                            soup = BeautifulSoup(content, "html.parser")
                            text = soup.get_text(separator="\n", strip=True)
                    except Exception as e:
                        logger.debug(f"[WebCrawlerSource] HTML parsing error for {url}: {e}")
                        soup = BeautifulSoup(content, "html.parser")
                        text = soup.get_text(separator="\n", strip=True)

                    score = self._score_text(text or "", content)
                    self.results.append({
                        "file_name": file_name,
                        "binary": content,
                        "raw_text": text or "",
                        "source_path": url,
                        "source_type": "web",
                        "user_metadata": self.user_metadata,
                        "score": score
                    })

                    # 递归：发现链接并加入队列（仅当 depth < max_depth）
                    if depth < self.max_depth:
                        try:
                            soup = BeautifulSoup(content, "html.parser")
                            for link in soup.find_all("a", href=True):
                                href = link["href"]
                                abs_url = urljoin(url, href)
                                abs_url = _normalize_url(abs_url)
                                if abs_url in self.seen_urls:
                                    continue
                                if _is_ad_link(abs_url):
                                    continue
                                # 仅当在 scope 内才放入队列
                                if not self._is_url_in_scope(abs_url):
                                    continue
                                self.seen_urls.add(abs_url)
                                await queue.put((abs_url, depth + 1))
                        except Exception as e:
                            logger.debug(f"[WebCrawlerSource] Link extraction error for {url}: {e}")

                # 二进制文件（PDF/Office等）—— 返回 binary，由 Tika 处理
                elif ext in self.allowed_extensions or "application/pdf" in content_type or "application/octet-stream" in content_type:
                    self.results.append({
                        "file_name": file_name,
                        "binary": content,
                        "source_path": url,
                        "source_type": "web",
                        "user_metadata": self.user_metadata,
                        "score": 0.0
                    })
                else:
                    logger.debug(f"[WebCrawlerSource] Skipped content-type {content_type} for {url}")
        except Exception as e:
            logger.error(f"[WebCrawlerSource] Failed to fetch/process {url}: {e}")

    def _score_text(self, text: str, raw_bytes: bytes) -> float:
        """
        基本评分函数：基于正文长度与 HTML 大小的比值。
        越长越好，简单启发式评分：
            score = min(1.0, (len(text) / max(1, len(raw_bytes))) * 10)
        也可根据需要扩展（阅读深度、关键字匹配等）
        """
        try:
            ratio = len(text) / max(1, len(raw_bytes))
            score = min(1.0, ratio * 10)
            return round(score, 4)
        except Exception:
            return 0.0
