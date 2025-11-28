import re

class CleanProcessor:
    def process(self, data):
        text = data["raw_text"]
        text = re.sub(r"\s+", " ", text)
        return { "clean_text": text }
