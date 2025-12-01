import uuid
import hashlib

def generate_professional_uuid_id(
    doc_id: str,
    namespace_seed: str = "com.geelink.2025"  # 随便填，保持一致就行
) -> str:
    """
    根据 doc_id 生成一个固定、专业的 UUID5
    相同 doc_id → 永远同一个 UUID → Solr 100% 覆盖
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{namespace_seed}:{doc_id}"))