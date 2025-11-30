# app/sinks/solr_sink.py
from typing import Dict, Any, Optional
from app.sinks.base import BaseSink
import requests
from app.utility.config import Config  # 假设你已有统一 config
from app.utility.log import logger

class SolrSink(BaseSink):
    def __init__(self, solr_url: Optional[str] = None):
        self.solr_url = solr_url or Config.SOLR_URL

    def write(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> None:
        logger.info(f"context:{context}")
        del data['binary']
        logger.info(f"data:{data}")
        # 构建文档：示例把 raw_text/clean_text 放到 Solr doc 中
        doc = {
            "id": context.get("doc_id") or data.get("filename"),
            "raw_text": data.get("raw_text"),
            "clean_text": data.get("clean_text"),
            # 你可以按自己的 schema 添加 fields
        }
        if not self.solr_url:
            # no-op or log
            return
        try:
            resp = requests.post(self.solr_url, json=[doc], params={"commit": "true"}, timeout=10)
            resp.raise_for_status()
        except Exception:
            # 这里不要 raise（或者根据需要决定），以免阻塞整个 pipeline
            # 建议记录日志
            pass
