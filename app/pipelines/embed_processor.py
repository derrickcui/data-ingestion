from openai import OpenAI
client = OpenAI(api_key="sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

class EmbedProcessor:
    def process(self, data):
        embeddings = []

        for chunk in data["chunks"]:
            vec = client.embeddings.create(
                model="text-embedding-3-small",
                input=chunk
            ).data[0].embedding

            embeddings.append({"text": chunk, "embedding": vec})

        return { "embeddings": embeddings }
