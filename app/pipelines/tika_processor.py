import requests
from app.utility.log import logger

class TikaProcessor:
    def process(self, source_data):
        r = requests.put(
            "http://localhost:9998/tika",
            data=source_data["binary"],
            headers={"Accept": "text/plain"}
        )

        logger.info(r.text)
        return {"raw_text": r.text}
