from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AIVocabularyPromptVersion(Base):
    __tablename__ = "ai_vocabulary_prompt_versions"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: f"pv_{uuid4().hex[:24]}")
    prompt_version: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(128), default="Vocabulary Extract Prompt")
    description: Mapped[str] = mapped_column(Text, default="")
    system_prompt: Mapped[str] = mapped_column(Text)
    user_prompt_template: Mapped[str] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class DatasetSampleVersion(Base):
    __tablename__ = "dataset_sample_versions"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: f"sv_{uuid4().hex[:24]}")
    dataset: Mapped[str] = mapped_column(String(128), index=True)
    version_name: Mapped[str] = mapped_column(String(64), index=True)
    sample_type: Mapped[str] = mapped_column(String(32), default="BASE")
    generation_strategy: Mapped[str] = mapped_column(String(64), default="diversity_sampling")
    status: Mapped[str] = mapped_column(String(32), default="READY")
    candidate_pool_size: Mapped[int] = mapped_column(Integer, default=0)
    final_sample_size: Mapped[int] = mapped_column(Integer, default=0)
    similarity_threshold: Mapped[float] = mapped_column(Float, default=0.85)
    max_chunks_per_doc: Mapped[int] = mapped_column(Integer, default=2)
    avg_similarity: Mapped[float | None] = mapped_column(Float, nullable=True)
    min_similarity: Mapped[float | None] = mapped_column(Float, nullable=True)
    cluster_count_estimate: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    items: Mapped[list["DatasetSampleItem"]] = relationship(
        back_populates="sample_version",
        cascade="all, delete-orphan",
    )


class DatasetSampleItem(Base):
    __tablename__ = "dataset_sample_items"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: f"si_{uuid4().hex[:24]}")
    sample_version_id: Mapped[str] = mapped_column(ForeignKey("dataset_sample_versions.id"), index=True)
    dataset: Mapped[str] = mapped_column(String(128), index=True)
    doc_id: Mapped[str] = mapped_column(String(128), index=True)
    chunk_id: Mapped[str] = mapped_column(String(128), index=True)
    title: Mapped[str] = mapped_column(Text, default="")
    anchor_content: Mapped[str] = mapped_column(Text)
    sample_content: Mapped[str] = mapped_column(Text)
    chunk_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_name: Mapped[str] = mapped_column(Text, default="")
    quality_score: Mapped[float] = mapped_column(Float, default=0.0)
    diversity_score: Mapped[float] = mapped_column(Float, default=0.0)
    selection_reason: Mapped[str] = mapped_column(String(128), default="diversity")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    sample_version: Mapped["DatasetSampleVersion"] = relationship(back_populates="items")


class AIVocabularyRun(Base):
    __tablename__ = "ai_vocabulary_runs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: f"run_{uuid4().hex[:24]}")
    dataset: Mapped[str] = mapped_column(String(128), index=True)
    sample_version_id: Mapped[str] = mapped_column(ForeignKey("dataset_sample_versions.id"), index=True)
    run_key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    prompt_version: Mapped[str] = mapped_column(String(64), default="vocab_extract_v1")
    provider: Mapped[str] = mapped_column(String(64), default="openai")
    model_name: Mapped[str] = mapped_column(String(128), default="")
    status: Mapped[str] = mapped_column(String(32), default="CREATED")
    temperature: Mapped[float] = mapped_column(Float, default=0.1)
    batch_size: Mapped[int] = mapped_column(Integer, default=10)
    total_samples: Mapped[int] = mapped_column(Integer, default=0)
    processed_samples: Mapped[int] = mapped_column(Integer, default=0)
    total_terms: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_progress_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class AIVocabularyRunLog(Base):
    __tablename__ = "ai_vocabulary_run_logs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: f"rl_{uuid4().hex[:24]}")
    ai_run_id: Mapped[str] = mapped_column(ForeignKey("ai_vocabulary_runs.id"), index=True)
    level: Mapped[str] = mapped_column(String(16), default="INFO", index=True)
    message: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, index=True)


class AIVocabularyTermRaw(Base):
    __tablename__ = "ai_vocabulary_terms_raw"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: f"rt_{uuid4().hex[:24]}")
    ai_run_id: Mapped[str] = mapped_column(ForeignKey("ai_vocabulary_runs.id"), index=True)
    sample_item_id: Mapped[str] = mapped_column(ForeignKey("dataset_sample_items.id"), index=True)
    dataset: Mapped[str] = mapped_column(String(128), index=True)
    doc_id: Mapped[str] = mapped_column(String(128), index=True)
    chunk_id: Mapped[str] = mapped_column(String(128), index=True)
    term: Mapped[str] = mapped_column(String(512), index=True)
    normalized_term: Mapped[str] = mapped_column(String(512), index=True)
    evidence: Mapped[str] = mapped_column(Text, default="")
    evidence_start: Mapped[int | None] = mapped_column(Integer, nullable=True)
    evidence_end: Mapped[int | None] = mapped_column(Integer, nullable=True)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    validation_status: Mapped[str] = mapped_column(String(32), default="VALID", index=True)
    ignored_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    ignore_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ignore_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_model_output: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class TermCandidate(Base):
    __tablename__ = "term_candidate"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    canonical: Mapped[str] = mapped_column(Text, index=True)
    aliases: Mapped[list] = mapped_column(JSONB, default=list)
    role: Mapped[str | None] = mapped_column(Text, nullable=True)
    definition: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, default="CANDIDATE")
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    source: Mapped[str | None] = mapped_column(Text, nullable=True)
    owner: Mapped[str | None] = mapped_column(Text, nullable=True)
    topics: Mapped[list] = mapped_column(JSONB, default=list)
    version: Mapped[int | None] = mapped_column(Integer, default=1)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    submitted_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    reviewed_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    review_comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=utc_now, onupdate=utc_now)
    evidence: Mapped[list] = mapped_column(JSONB, default=list)
