import chromadb

class ChromaSink:
    def __init__(self):
        self.db = chromadb.Client()

    def write(self, data):
        col = self.db.get_or_create_collection("documents")

        for item in data["embeddings"]:
            col.add(
                documents=[item["text"]],
                embeddings=[item["embedding"]]
            )
