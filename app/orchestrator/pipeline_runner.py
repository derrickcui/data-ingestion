from typing import Dict, Any, List

class PipelineRunner:
    """
    Orchestrator: Source → Processors → Sinks
    固定版本，不再变更。
    """

    def __init__(self, source, processors: List, sinks: List):
        self.source = source
        self.processors = processors
        self.sinks = sinks

    def run(self, context: Dict[str, Any] = None) -> Dict[str, Any]:

        context = context or {}

        # 1. Source 读取初始数据
        data = self.source.read(context=context)

        # 2. processors 顺序处理，每一步返回全新对象
        for processor in self.processors:
            data = processor.process(data, context=context)

        # 3. sinks 写入到 Solr / Chroma / Nebula / DB
        for sink in self.sinks:
            sink.write(data, context=context)

        return data
