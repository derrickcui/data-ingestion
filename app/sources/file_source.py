class FileSource:
    def __init__(self, filename: str, content: bytes):
        self.filename = filename
        self.content = content

    def read(self, context=None):
        """
        context 不一定需要使用，但必须接受，以兼容 Pipeline 调用规范。
        """
        return {
            "filename": self.filename,
            "content": self.content
        }
