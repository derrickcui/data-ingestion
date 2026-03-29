from datetime import datetime

from pydantic import BaseModel, Field


class GenerateSampleRequest(BaseModel):
    dataset: str = Field(..., description="Dataset identifier")
    version_name: str = Field(..., description="Frozen sample version name, e.g. v1")
    sample_type: str = Field(default="BASE", description="BASE | TOPIC | BLIND")
    candidate_pool_size: int = Field(default=2000, ge=100, le=20000)
    sample_size: int = Field(default=150, ge=10, le=1000)
    similarity_threshold: float = Field(default=0.85, ge=0.0, le=1.0)
    max_chunks_per_doc: int = Field(default=2, ge=1, le=10)


class SampleVersionResponse(BaseModel):
    id: str
    dataset: str
    version_name: str
    sample_type: str
    generation_strategy: str
    status: str
    candidate_pool_size: int
    final_sample_size: int
    similarity_threshold: float
    max_chunks_per_doc: int = 2
    avg_similarity: float | None = None
    min_similarity: float | None = None
    cluster_count_estimate: int | None = None
    created_at: datetime

    class Config:
        from_attributes = True


class SampleItemResponse(BaseModel):
    id: str
    sample_version_id: str
    dataset: str
    doc_id: str
    chunk_id: str
    title: str
    anchor_content: str
    sample_content: str
    chunk_index: int | None = None
    source_name: str
    quality_score: float
    diversity_score: float
    selection_reason: str
    created_at: datetime

    class Config:
        from_attributes = True


class CreateRunRequest(BaseModel):
    dataset: str
    sample_version_id: str
    prompt_version: str = "vocab_extract_v1"
    provider: str = "openai"
    model_name: str = ""
    temperature: float = Field(default=0.1, ge=0.0, le=2.0)
    batch_size: int = Field(default=10, ge=1, le=50)


class RunResponse(BaseModel):
    id: str
    dataset: str
    sample_version_id: str
    run_key: str
    prompt_version: str
    provider: str
    model_name: str
    status: str
    temperature: float
    batch_size: int
    total_samples: int
    processed_samples: int
    total_terms: int
    started_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    last_progress_message: str | None = None
    created_at: datetime
    finished_at: datetime | None = None

    class Config:
        from_attributes = True


class RunLogResponse(BaseModel):
    id: str
    ai_run_id: str
    level: str
    message: str
    created_at: datetime

    class Config:
        from_attributes = True


class RawTermResponse(BaseModel):
    id: str
    ai_run_id: str
    sample_item_id: str
    dataset: str
    doc_id: str
    chunk_id: str
    term: str
    normalized_term: str
    evidence: str
    evidence_start: int | None = None
    evidence_end: int | None = None
    confidence: float
    validation_status: str
    raw_model_output: str
    created_at: datetime

    class Config:
        from_attributes = True


class TermCandidateResponse(BaseModel):
    id: int
    dataset: str | None = None
    term: str
    normalized_term: str
    source: str
    ai_run_id: str | None = None
    sample_version_id: str | None = None
    doc_id: str | None = None
    evidence: str
    confidence: float
    evidence_count: int = 0
    document_count: int = 0
    status: str
    reviewed: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ReviewCandidateRequest(BaseModel):
    status: str = Field(..., description="APPROVED | REJECTED | CANDIDATE")


class RunSummaryResponse(BaseModel):
    run_id: str
    run_key: str
    dataset: str
    sample_version_id: str
    status: str
    total_samples: int
    processed_samples: int
    total_terms: int
    last_heartbeat_at: datetime | None = None
    last_progress_message: str | None = None
    raw_term_count: int
    valid_term_count: int
    invalid_term_count: int
    candidate_count: int


class TermCandidateEvidenceResponse(BaseModel):
    raw_term_id: str | None = None
    doc_id: str | None = None
    chunk_id: str | None = None
    term: str
    evidence: str
    confidence: float
    validation_status: str
    created_at: datetime


class CreatePromptVersionRequest(BaseModel):
    prompt_version: str = Field(..., description="Unique prompt version, e.g. vocab_extract_v1")
    name: str = Field(default="Vocabulary Extract Prompt")
    description: str = Field(default="")
    system_prompt: str = Field(..., description="System prompt content")
    user_prompt_template: str = Field(
        ...,
        description="User prompt template. Use {sample_content} placeholder for sample text.",
    )
    is_active: bool = True


class PromptVersionResponse(BaseModel):
    id: str
    prompt_version: str
    name: str
    description: str
    system_prompt: str
    user_prompt_template: str
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
