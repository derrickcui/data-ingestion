import uuid
import requests

class SolrSink:
    SOLR_URL = "http://localhost:8983/solr/mycore/update?commit=true"

    def write(self, data):
        doc = {
            "id": str(uuid.uuid4()),
            "raw_text": data["raw_text"],
            "clean_text": data["clean_text"],
            "metadata": data["metadata"],
        }

        r = requests.post(self.SOLR_URL, json=[doc])
        return r.json()
