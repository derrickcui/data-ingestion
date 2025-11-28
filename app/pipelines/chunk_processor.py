class ChunkProcessor:
    def __init__(self, max_len=300):
        self.max_len = max_len

    def process(self, data):
        words = data["clean_text"].split()
        chunks, buf = [], []

        for w in words:
            buf.append(w)
            if len(buf) >= self.max_len:
                chunks.append(" ".join(buf))
                buf = []

        if buf:
            chunks.append(" ".join(buf))

        return { "chunks": chunks }
