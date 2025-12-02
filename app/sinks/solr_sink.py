# app/sinks/solr_sink.py

from typing import Dict, Any, Optional
from app.sinks.base import BaseSink
import requests
from app.utility.config import Config  # 假设你已有统一 config
from app.utility.log import logger

class SolrSink(BaseSink):
    def __init__(self, solr_url: Optional[str] = None):
        self.solr_url = solr_url or Config.SOLR_URL
        self.update_url = f"{self.solr_url}/solr/shediao/update"

    def write(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> None:
        docs = data["solr_docs"]          # 1 document + N chunks
        #logger.info(f"docs:{docs}")
        if not self.solr_url:
            # no-op or log
            return
        """
        logger.info("处理完成，solr doc 如下：\n%s",
                    json.dumps(docs, ensure_ascii=False, indent=2))
        """
        logger.info(f"Starting to persist data, total number of docs:{len(docs or [])}")
        try:
            resp = requests.post(self.update_url, json=docs, params={"commit": "true"}, timeout=10)
            resp.raise_for_status()
            logger.info("completed to persist data")
        except Exception as e:
            logger.error(f"solr sink failed: {e}")
            # 这里不要 raise（或者根据需要决定），以免阻塞整个 pipeline
            # 建议记录日志
            raise
