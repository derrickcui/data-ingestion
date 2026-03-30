from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from datetime import datetime, timezone
from math import sqrt
import re
from statistics import mean
from typing import Any

import requests
from sqlalchemy import and_, asc, desc, func, or_, select
from sqlalchemy.orm import Session

from app.ai_providers.aliyun_client import AliEmbeddingClient
from app.ai_providers.aliyun_llm_client import AliyunLLMClient
from app.ai_providers.google_client import GoogleEmbeddingClient
from app.ai_providers.google_llm_client import GoogleLLMClient
from app.ai_providers.openai_client import OpenAIEmbeddingClient
from app.ai_providers.openai_llm_client import OpenAILLMClient
from app.ai_vocabulary.models import (
    AIVocabularyPromptVersion,
    AIVocabularyRun,
    AIVocabularyRunLog,
    AIVocabularyTermRaw,
    DatasetSampleItem,
    DatasetSampleVersion,
    TermCandidate,
)
from app.ai_vocabulary.schemas import CreateRunRequest, GenerateSampleRequest
from app.utility.config import Config
from app.utility.log import logger

LOW_CONFIDENCE_THRESHOLD = 0.6
RUN_TERM_SORT_FIELDS = {
    "createdAt": AIVocabularyTermRaw.created_at,
    "confidence": AIVocabularyTermRaw.confidence,
    "term": AIVocabularyTermRaw.term,
    "normalizedTerm": AIVocabularyTermRaw.normalized_term,
    "validationStatus": AIVocabularyTermRaw.validation_status,
    "docId": AIVocabularyTermRaw.doc_id,
}
VALIDATION_STATUS_COMPATIBILITY = {
    "VALID": {"VALID"},
    "INVALID_JSON": {"INVALID_JSON", "INVALID_SCHEMA", "INVALID_MODEL_OUTPUT"},
    "INVALID_EVIDENCE": {"INVALID_EVIDENCE", "INVALID_TERM_MISMATCH"},
    "FILTERED_NOISE": {"FILTERED_NOISE", "FILTERED_PLACEHOLDER"},
    "LOW_CONFIDENCE": {"LOW_CONFIDENCE"},
}


def normalize_validation_status(status: str | None, confidence: float | None = None) -> str:
    normalized = (status or "").strip().upper()
    if normalized == "VALID" and confidence is not None and confidence < LOW_CONFIDENCE_THRESHOLD:
        return "LOW_CONFIDENCE"
    if normalized in VALIDATION_STATUS_COMPATIBILITY["INVALID_JSON"]:
        return "INVALID_JSON"
    if normalized in VALIDATION_STATUS_COMPATIBILITY["INVALID_EVIDENCE"]:
        return "INVALID_EVIDENCE"
    if normalized in VALIDATION_STATUS_COMPATIBILITY["FILTERED_NOISE"]:
        return "FILTERED_NOISE"
    if normalized in VALIDATION_STATUS_COMPATIBILITY["LOW_CONFIDENCE"]:
        return "LOW_CONFIDENCE"
    return normalized or "INVALID_JSON"


@dataclass
class ChunkCandidate:
    chunk_id: str
    doc_id: str
    logical_doc_id: str
    title: str
    content: str
    vector: list[float]
    chunk_index: int | None
    source_name: str
    quality_score: float


def _normalize_vector(raw_vector: Any) -> list[float]:
    if isinstance(raw_vector, list):
        return [float(v) for v in raw_vector]
    return []


def _cosine_similarity(vec1: list[float], vec2: list[float]) -> float:
    if not vec1 or not vec2 or len(vec1) != len(vec2):
        return 0.0
    dot = sum(a * b for a, b in zip(vec1, vec2))
    norm1 = sqrt(sum(a * a for a in vec1))
    norm2 = sqrt(sum(b * b for b in vec2))
    if norm1 == 0.0 or norm2 == 0.0:
        return 0.0
    return dot / (norm1 * norm2)


def _quality_score(content: str) -> float:
    text = (content or "").strip()
    if not text:
        return 0.0

    length = len(text)
    score = 0.0
    if 120 <= length <= 1200:
        score += 0.45
    elif 80 <= length < 120 or 1200 < length <= 1800:
        score += 0.25

    alnum_ratio = sum(ch.isalnum() for ch in text) / max(length, 1)
    punctuation_ratio = sum(ch in "，。；：！？,.!?;:" for ch in text) / max(length, 1)
    if alnum_ratio >= 0.55:
        score += 0.35
    if 0.01 <= punctuation_ratio <= 0.12:
        score += 0.20

    return min(score, 1.0)


def _split_sentences(text: str) -> list[str]:
    normalized = re.sub(r"\s+", " ", (text or "").strip())
    if not normalized:
        return []
    parts = re.split(r"(?<=[。！？；.!?;])\s*", normalized)
    sentences = [part.strip() for part in parts if part and part.strip()]
    return sentences or ([normalized] if normalized else [])


def _sentence_score(sentence: str) -> float:
    text = (sentence or "").strip()
    if not text:
        return 0.0
    length = len(text)
    score = 0.0
    if 18 <= length <= 120:
        score += 0.5
    elif 8 <= length < 18 or 120 < length <= 180:
        score += 0.25
    alnum_ratio = sum(ch.isalnum() for ch in text) / max(length, 1)
    punctuation_ratio = sum(ch in "，。；：！？,.!?;:" for ch in text) / max(length, 1)
    if alnum_ratio >= 0.55:
        score += 0.35
    if 0.01 <= punctuation_ratio <= 0.16:
        score += 0.15
    return min(score, 1.0)


def _logical_doc_id(doc_id: str) -> str:
    value = (doc_id or "").strip()
    match = re.match(r"^(.*)_chunk_\d+$", value)
    return match.group(1) if match else value


class SampleGenerationService:
    def __init__(self, db: Session):
        self.db = db

    def generate_base_sample(self, request: GenerateSampleRequest) -> DatasetSampleVersion:
        existing = self.db.scalar(
            select(DatasetSampleVersion).where(
                DatasetSampleVersion.dataset == request.dataset,
                DatasetSampleVersion.version_name == request.version_name,
                DatasetSampleVersion.sample_type == request.sample_type,
            )
        )
        if existing is not None:
            raise ValueError(
                f"Sample version already exists for dataset={request.dataset}, "
                f"version={request.version_name}, sample_type={request.sample_type}"
            )

        candidates = self._load_chunk_candidates(request.candidate_pool_size)
        selected, selected_scores = self._select_candidates_with_doc_coverage(
            candidates=candidates,
            sample_size=request.sample_size,
            similarity_threshold=request.similarity_threshold,
            max_chunks_per_doc=request.max_chunks_per_doc,
        )

        similarity_samples = [score for score in selected_scores if score > 0.0]
        avg_similarity = mean(similarity_samples) if similarity_samples else 0.0
        min_similarity = min(similarity_samples) if similarity_samples else 0.0
        cluster_count_estimate = len(
            {
                max(0, min(int(score / 0.1), 9))
                for score in selected_scores
            }
        ) or len(selected)

        sample_version = DatasetSampleVersion(
            dataset=request.dataset,
            version_name=request.version_name,
            sample_type=request.sample_type,
            generation_strategy="diversity_sampling",
            status="READY",
            candidate_pool_size=len(candidates),
            final_sample_size=len(selected),
            similarity_threshold=request.similarity_threshold,
            max_chunks_per_doc=request.max_chunks_per_doc,
            avg_similarity=avg_similarity,
            min_similarity=min_similarity,
            cluster_count_estimate=cluster_count_estimate,
        )
        self.db.add(sample_version)
        self.db.flush()

        for idx, candidate in enumerate(selected):
            sample_content = self._build_sentence_window(candidate.doc_id, candidate.chunk_index, candidate.content)
            self.db.add(
                DatasetSampleItem(
                    sample_version_id=sample_version.id,
                    dataset=request.dataset,
                    doc_id=candidate.doc_id,
                    chunk_id=candidate.chunk_id,
                    title=candidate.title,
                    anchor_content=candidate.content,
                    sample_content=sample_content,
                    chunk_index=candidate.chunk_index,
                    source_name=candidate.source_name,
                    quality_score=candidate.quality_score,
                    diversity_score=selected_scores[idx],
                    selection_reason="diversity",
                )
            )

        self.db.commit()
        self.db.refresh(sample_version)
        return sample_version

    def _select_candidates_with_doc_coverage(
        self,
        candidates: list[ChunkCandidate],
        sample_size: int,
        similarity_threshold: float,
        max_chunks_per_doc: int,
    ) -> tuple[list[ChunkCandidate], list[float]]:
        ordered = sorted(candidates, key=lambda item: item.quality_score, reverse=True)
        selected: list[ChunkCandidate] = []
        selected_scores: list[float] = []
        doc_counts: dict[str, int] = {}

        for doc_cap in range(1, max_chunks_per_doc + 1):
            for candidate in ordered:
                if len(selected) >= sample_size:
                    break
                if any(item.chunk_id == candidate.chunk_id for item in selected):
                    continue
                if doc_counts.get(candidate.logical_doc_id, 0) >= doc_cap:
                    continue
                max_similarity = max(
                    (_cosine_similarity(candidate.vector, chosen.vector) for chosen in selected),
                    default=0.0,
                )
                if max_similarity >= similarity_threshold:
                    continue
                selected.append(candidate)
                selected_scores.append(max_similarity)
                doc_counts[candidate.logical_doc_id] = doc_counts.get(candidate.logical_doc_id, 0) + 1
            if len(selected) >= sample_size:
                break

        if len(selected) < sample_size:
            for candidate in ordered:
                if len(selected) >= sample_size:
                    break
                if any(item.chunk_id == candidate.chunk_id for item in selected):
                    continue
                max_similarity = max(
                    (_cosine_similarity(candidate.vector, chosen.vector) for chosen in selected),
                    default=0.0,
                )
                if max_similarity >= similarity_threshold:
                    continue
                selected.append(candidate)
                selected_scores.append(max_similarity)

        return selected, selected_scores

    def _load_chunk_candidates(self, rows: int) -> list[ChunkCandidate]:
        solr_url = f"{Config.SOLR_URL}/solr/{Config.SOLR_COLLECTION}/select"
        response = requests.get(
            solr_url,
            params={
                "q": "doc_type:chunk",
                "rows": rows,
                "wt": "json",
                "fl": "id,doc_id,title,chunk_content,chunk_index,source_name,_gl_vector",
            },
            timeout=30,
        )
        response.raise_for_status()
        docs = response.json().get("response", {}).get("docs", [])

        candidates: list[ChunkCandidate] = []
        for doc in docs:
            content = (doc.get("chunk_content") or "").strip()
            vector = _normalize_vector(doc.get("_gl_vector"))
            if len(content) < 80 or not vector:
                continue

            score = _quality_score(content)
            if score < 0.3:
                continue

            candidates.append(
                ChunkCandidate(
                    chunk_id=str(doc.get("id", "")),
                    doc_id=str(doc.get("doc_id", "")),
                    logical_doc_id=_logical_doc_id(str(doc.get("doc_id", ""))),
                    title=str(doc.get("title", "")),
                    content=content,
                    vector=vector,
                    chunk_index=doc.get("chunk_index"),
                    source_name=str(doc.get("source_name", "")),
                    quality_score=score,
                )
            )
        return candidates

    def _build_sentence_window(self, doc_id: str, chunk_index: int | None, fallback_content: str) -> str:
        if not doc_id or chunk_index is None:
            return self._build_local_sentence_window(fallback_content)
        logical_doc_id = _logical_doc_id(doc_id)

        solr_url = f"{Config.SOLR_URL}/solr/{Config.SOLR_COLLECTION}/select"
        response = requests.get(
            solr_url,
            params={
                "q": f'doc_type:chunk AND doc_id:{logical_doc_id}_chunk_*',
                "rows": 100,
                "wt": "json",
                "sort": "chunk_index asc",
                "fl": "chunk_index,chunk_content",
            },
            timeout=20,
        )
        response.raise_for_status()
        docs = response.json().get("response", {}).get("docs", [])

        by_index = {
            int(item["chunk_index"]): (item.get("chunk_content") or "").strip()
            for item in docs
            if item.get("chunk_index") is not None
        }
        prev_text = by_index.get(chunk_index - 1, "")
        current_text = by_index.get(chunk_index, fallback_content)
        next_text = by_index.get(chunk_index + 1, "")
        return self._compose_sentence_window(prev_text, current_text, next_text)

    def _build_local_sentence_window(self, content: str) -> str:
        return self._compose_sentence_window("", content, "")

    def _compose_sentence_window(self, prev_text: str, current_text: str, next_text: str) -> str:
        current_sentences = _split_sentences(current_text)
        if not current_sentences:
            return current_text

        anchor_idx = max(
            range(len(current_sentences)),
            key=lambda idx: _sentence_score(current_sentences[idx]),
        )
        selected: list[str] = []

        prev_sentences = _split_sentences(prev_text)
        if anchor_idx == 0 and prev_sentences:
            selected.append(prev_sentences[-1])
        elif anchor_idx > 0:
            selected.append(current_sentences[anchor_idx - 1])

        selected.append(current_sentences[anchor_idx])

        next_sentences = _split_sentences(next_text)
        if anchor_idx < len(current_sentences) - 1:
            selected.append(current_sentences[anchor_idx + 1])
        elif next_sentences:
            selected.append(next_sentences[0])

        return " ".join(sentence.strip() for sentence in selected if sentence and sentence.strip()) or current_text


class PromptVersionService:
    DEFAULT_SYSTEM_PROMPT = (
        "你是企业术语抽取器。"
        "你只能基于提供的文本抽取术语，不能生成原文中不存在的概念。"
        "必须输出结构化 JSON。"
    )
    DEFAULT_USER_PROMPT_TEMPLATE = (
        "请从下面文本中抽取可用于规则构建的关键短语。"
        "每个术语必须附带原文中的连续证据片段。"
        "输出 JSON 数组，格式为 "
        '[{{"term":"术语","evidence":"原文证据","confidence":0.95}}]。'
        "如果没有合适术语，输出 []。\n\n"
        "文本如下：\n{sample_content}"
    )

    def __init__(self, db: Session):
        self.db = db

    def ensure_default_prompt(self) -> AIVocabularyPromptVersion:
        prompt = self.db.scalar(
            select(AIVocabularyPromptVersion).where(
                AIVocabularyPromptVersion.prompt_version == "vocab_extract_v1"
            )
        )
        if prompt is not None:
            prompt.name = "Default Vocabulary Extract Prompt"
            prompt.description = "Default prompt for AI vocabulary extraction phase 1"
            prompt.system_prompt = self.DEFAULT_SYSTEM_PROMPT
            prompt.user_prompt_template = self.DEFAULT_USER_PROMPT_TEMPLATE
            prompt.is_active = True
            self.db.commit()
            self.db.refresh(prompt)
            return prompt

        prompt = AIVocabularyPromptVersion(
            prompt_version="vocab_extract_v1",
            name="Default Vocabulary Extract Prompt",
            description="Default prompt for AI vocabulary extraction phase 1",
            system_prompt=self.DEFAULT_SYSTEM_PROMPT,
            user_prompt_template=self.DEFAULT_USER_PROMPT_TEMPLATE,
            is_active=True,
        )
        self.db.add(prompt)
        self.db.commit()
        self.db.refresh(prompt)
        return prompt

    def create_prompt_version(
        self,
        prompt_version: str,
        name: str,
        description: str,
        system_prompt: str,
        user_prompt_template: str,
        is_active: bool,
    ) -> AIVocabularyPromptVersion:
        existing = self.db.scalar(
            select(AIVocabularyPromptVersion).where(
                AIVocabularyPromptVersion.prompt_version == prompt_version
            )
        )
        if existing is not None:
            raise ValueError(f"Prompt version already exists: {prompt_version}")

        prompt = AIVocabularyPromptVersion(
            prompt_version=prompt_version,
            name=name,
            description=description,
            system_prompt=system_prompt,
            user_prompt_template=user_prompt_template,
            is_active=is_active,
        )
        self.db.add(prompt)
        self.db.commit()
        self.db.refresh(prompt)
        return prompt

    def list_prompt_versions(self) -> list[AIVocabularyPromptVersion]:
        return list(
            self.db.scalars(
                select(AIVocabularyPromptVersion).order_by(AIVocabularyPromptVersion.created_at.desc())
            ).all()
        )

    def get_prompt_version(self, prompt_version: str) -> AIVocabularyPromptVersion:
        prompt = self.db.scalar(
            select(AIVocabularyPromptVersion).where(
                AIVocabularyPromptVersion.prompt_version == prompt_version
            )
        )
        if prompt is None:
            raise ValueError(f"Prompt version not found: {prompt_version}")
        return prompt


class AIVocabularyRunService:
    def __init__(self, db: Session):
        self.db = db

    def list_run_logs(self, run_id: str, limit: int = 200) -> list[AIVocabularyRunLog]:
        stmt = (
            select(AIVocabularyRunLog)
            .where(AIVocabularyRunLog.ai_run_id == run_id)
            .order_by(AIVocabularyRunLog.created_at.asc())
            .limit(max(1, min(limit, 1000)))
        )
        return list(self.db.scalars(stmt).all())

    def list_runs(
        self,
        dataset: str | None = None,
        status: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[AIVocabularyRun]:
        stmt = select(AIVocabularyRun).order_by(AIVocabularyRun.created_at.desc())
        if dataset:
            stmt = stmt.where(AIVocabularyRun.dataset == dataset)
        if status:
            stmt = stmt.where(AIVocabularyRun.status == status)
        stmt = stmt.offset(max(offset, 0)).limit(max(1, min(limit, 100)))
        return list(self.db.scalars(stmt).all())

    def create_run(self, request: CreateRunRequest) -> AIVocabularyRun:
        sample_version = self.db.get(DatasetSampleVersion, request.sample_version_id)
        if sample_version is None:
            raise ValueError(f"Sample version not found: {request.sample_version_id}")
        PromptVersionService(self.db).get_prompt_version(request.prompt_version)

        total_samples = self.db.scalar(
            select(func.count(DatasetSampleItem.id)).where(
                DatasetSampleItem.sample_version_id == request.sample_version_id
            )
        ) or 0

        run_key = self._build_run_key(
            dataset=request.dataset,
            sample_version_id=request.sample_version_id,
            prompt_version=request.prompt_version,
            provider=request.provider,
            model_name=request.model_name,
            temperature=request.temperature,
        )
        existing_run = self.db.scalar(
            select(AIVocabularyRun).where(AIVocabularyRun.run_key == run_key)
        )
        if existing_run is not None:
            return existing_run

        run = AIVocabularyRun(
            dataset=request.dataset,
            sample_version_id=request.sample_version_id,
            run_key=run_key,
            prompt_version=request.prompt_version,
            provider=request.provider,
            model_name=request.model_name,
            temperature=request.temperature,
            batch_size=request.batch_size,
            status="CREATED",
            total_samples=total_samples,
        )
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run

    def rerun(
        self,
        source_run_id: str,
        prompt_version: str | None = None,
        temperature: float | None = None,
        provider: str | None = None,
        model_name: str | None = None,
        batch_size: int | None = None,
    ) -> AIVocabularyRun:
        source_run = self.db.get(AIVocabularyRun, source_run_id)
        if source_run is None:
            raise ValueError(f"Run not found: {source_run_id}")

        effective_prompt_version = prompt_version or source_run.prompt_version
        PromptVersionService(self.db).get_prompt_version(effective_prompt_version)

        total_samples = self.db.scalar(
            select(func.count(DatasetSampleItem.id)).where(
                DatasetSampleItem.sample_version_id == source_run.sample_version_id
            )
        ) or 0
        base_run_key = self._build_run_key(
            dataset=source_run.dataset,
            sample_version_id=source_run.sample_version_id,
            prompt_version=effective_prompt_version,
            provider=provider or source_run.provider,
            model_name=model_name if model_name is not None else source_run.model_name,
            temperature=temperature if temperature is not None else source_run.temperature,
        )
        unique_seed = f"{source_run_id}|{datetime.now(timezone.utc).isoformat()}"
        run = AIVocabularyRun(
            dataset=source_run.dataset,
            sample_version_id=source_run.sample_version_id,
            run_key=f"{base_run_key}_{hashlib.md5(unique_seed.encode('utf-8')).hexdigest()[:8]}",
            prompt_version=effective_prompt_version,
            provider=provider or source_run.provider,
            model_name=model_name if model_name is not None else source_run.model_name,
            temperature=temperature if temperature is not None else source_run.temperature,
            batch_size=batch_size if batch_size is not None else source_run.batch_size,
            status="CREATED",
            total_samples=total_samples,
        )
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run

    def execute_run(self, run_id: str) -> AIVocabularyRun:
        run = self.db.get(AIVocabularyRun, run_id)
        if run is None:
            raise ValueError(f"Run not found: {run_id}")
        if run.status == "COMPLETED":
            if not run.last_progress_message:
                run.last_progress_message = (
                    f"Completed run with {run.processed_samples}/{run.total_samples} samples and "
                    f"{run.total_terms} valid terms"
                )
                run.last_heartbeat_at = run.finished_at or run.created_at
                self.db.commit()
            logger.info(
                "AI vocabulary run already completed: run_id=%s run_key=%s dataset=%s",
                run.id,
                run.run_key,
                run.dataset,
            )
            return run
        try:
            sample_items = list(
                self.db.scalars(
                    select(DatasetSampleItem).where(
                        DatasetSampleItem.sample_version_id == run.sample_version_id
                    ).order_by(DatasetSampleItem.created_at.asc())
                ).all()
            )
            client = self._initialize_llm_client(run.provider, run.model_name)
            prompt_version = PromptVersionService(self.db).get_prompt_version(run.prompt_version)

            self.db.query(AIVocabularyTermRaw).filter(AIVocabularyTermRaw.ai_run_id == run.id).delete()
            self._clear_run_term_candidates(run.id)

            run.status = "RUNNING"
            run.started_at = datetime.now(timezone.utc)
            run.last_heartbeat_at = run.started_at
            run.last_progress_message = f"Starting run with {len(sample_items)} samples"
            run.processed_samples = 0
            run.total_terms = 0
            self._append_run_log(
                run_id=run.id,
                level="INFO",
                message=(
                    f"Run started. dataset={run.dataset}, sample_version_id={run.sample_version_id}, "
                    f"provider={run.provider}, model={run.model_name}, batch_size={max(1, run.batch_size or 1)}, "
                    f"total_samples={len(sample_items)}"
                ),
            )
            self.db.commit()
            batch_size = max(1, run.batch_size or 1)
            logger.info(
                "AI vocabulary run started: run_id=%s run_key=%s dataset=%s sample_version_id=%s total_samples=%s provider=%s model=%s batch_size=%s",
                run.id,
                run.run_key,
                run.dataset,
                run.sample_version_id,
                len(sample_items),
                run.provider,
                run.model_name,
                batch_size,
            )

            aggregated: dict[str, dict[str, Any]] = {}
            total_terms = 0
            term_embedding_client = self._initialize_embedding_client(run.provider)
            term_embedding_model = self._embedding_model_for_provider(run.provider)
            term_embedding_cache: dict[str, list[float]] = {}
            existing_candidate_vectors = self._load_existing_candidate_vectors(
                embedding_client=term_embedding_client,
                embedding_model=term_embedding_model,
                embedding_cache=term_embedding_cache,
            )

            for start in range(0, len(sample_items), batch_size):
                batch = sample_items[start:start + batch_size]
                batch_changed_keys: set[str] = set()
                run.last_heartbeat_at = datetime.now(timezone.utc)
                run.last_progress_message = (
                    f"Processing batch {start + 1}-{min(start + len(batch), len(sample_items))} "
                    f"of {len(sample_items)}"
                )
                self._append_run_log(
                    run_id=run.id,
                    level="INFO",
                    message=(
                        f"Batch started. range={start + 1}-{min(start + len(batch), len(sample_items))}, "
                        f"processed={run.processed_samples}/{len(sample_items)}"
                    ),
                )
                self.db.commit()
                logger.info(
                    "AI vocabulary run batch started: run_id=%s batch_start=%s batch_end=%s processed=%s total=%s",
                    run.id,
                    start + 1,
                    min(start + len(batch), len(sample_items)),
                    run.processed_samples,
                    len(sample_items),
                )
                try:
                    batch_results = self._execute_batch(client, prompt_version, batch)
                except Exception:
                    self._append_run_log(
                        run_id=run.id,
                        level="WARNING",
                        message=(
                            f"Batch fallback to single-item execution. "
                            f"range={start + 1}-{min(start + len(batch), len(sample_items))}"
                        ),
                    )
                    logger.warning(
                        "AI vocabulary run batch fallback to single-item execution: run_id=%s batch_start=%s batch_size=%s",
                        run.id,
                        start + 1,
                        len(batch),
                    )
                    batch_results = {
                        item.id: self._execute_single_item(client, prompt_version, item)
                        for item in batch
                    }
                else:
                    if all(not result[0] for result in batch_results.values()):
                        self._append_run_log(
                            run_id=run.id,
                            level="WARNING",
                            message=(
                                f"Batch returned empty results. Fallback to single-item execution. "
                                f"range={start + 1}-{min(start + len(batch), len(sample_items))}"
                            ),
                        )
                        logger.warning(
                            "AI vocabulary run batch produced empty results, fallback to single-item execution: run_id=%s batch_start=%s batch_size=%s",
                            run.id,
                            start + 1,
                            len(batch),
                        )
                        batch_results = {
                            item.id: self._execute_single_item(client, prompt_version, item)
                            for item in batch
                        }

                for item in batch:
                    parsed_items, raw_output = batch_results.get(item.id, ([], ""))
                    valid_count = 0

                    for parsed in parsed_items:
                        term = str(parsed.get("term", "")).strip()
                        evidence = str(parsed.get("evidence", "")).strip()
                        confidence = self._normalize_confidence(parsed.get("confidence"))
                        normalized_term = self._normalize_term(term)
                        validation_status = self._validate_term(
                            item.sample_content,
                            term,
                            evidence,
                            raw_output,
                            confidence,
                        )
                        evidence_start, evidence_end = self._find_evidence_span(item.sample_content, evidence)

                        raw_term = AIVocabularyTermRaw(
                            ai_run_id=run.id,
                            sample_item_id=item.id,
                            dataset=run.dataset,
                            doc_id=item.doc_id,
                            chunk_id=item.chunk_id,
                            term=term,
                            normalized_term=normalized_term,
                            evidence=evidence,
                            evidence_start=evidence_start,
                            evidence_end=evidence_end,
                            confidence=confidence,
                            validation_status=validation_status,
                            raw_model_output=raw_output,
                        )
                        self.db.add(raw_term)
                        self.db.flush()

                        if validation_status != "VALID":
                            continue

                        valid_count += 1
                        aggregate_key = self._resolve_semantic_group(
                            aggregated=aggregated,
                            term=term,
                            normalized_term=normalized_term,
                            embedding_client=term_embedding_client,
                            embedding_model=term_embedding_model,
                            embedding_cache=term_embedding_cache,
                        )
                        batch_changed_keys.add(aggregate_key)
                        aggregate = aggregated.setdefault(
                            aggregate_key,
                            {
                                "term": term,
                                "normalized_term": aggregate_key,
                                "confidence_sum": 0.0,
                                "count": 0,
                                "doc_ids": set(),
                                "evidence_items": [],
                            },
                        )
                        if len(term) > len(aggregate["term"]):
                            aggregate["term"] = term
                        aggregate["confidence_sum"] += confidence
                        aggregate["count"] += 1
                        aggregate["doc_ids"].add(item.doc_id)
                        aggregate["evidence_items"].append(
                            {
                                "source": "ai_extract",
                                "dataset": run.dataset,
                                "ai_run_id": run.id,
                                "sample_version_id": run.sample_version_id,
                                "sample_item_id": item.id,
                                "raw_term_id": raw_term.id,
                                "doc_id": item.doc_id,
                                "chunk_id": item.chunk_id,
                                "evidence": evidence,
                                "confidence": confidence,
                                "created_at": datetime.now(timezone.utc).isoformat(),
                            }
                        )

                    total_terms += valid_count
                    run.processed_samples += 1
                    run.total_terms = total_terms
                    run.last_heartbeat_at = datetime.now(timezone.utc)
                    run.last_progress_message = (
                        f"Processed sample {run.processed_samples}/{len(sample_items)}; "
                        f"sample_valid_terms={valid_count}; total_terms={total_terms}"
                    )
                    self._append_run_log(
                        run_id=run.id,
                        level="INFO",
                        message=(
                            f"Sample completed. sample_item_id={item.id}, "
                            f"processed={run.processed_samples}/{len(sample_items)}, "
                            f"sample_valid_terms={valid_count}, total_terms={total_terms}"
                        ),
                    )
                    self.db.commit()
                    logger.info(
                        "AI vocabulary run sample completed: run_id=%s sample_item_id=%s processed=%s/%s valid_terms_in_sample=%s total_terms=%s",
                        run.id,
                        item.id,
                        run.processed_samples,
                        len(sample_items),
                        valid_count,
                        total_terms,
                    )

                if batch_changed_keys:
                    for aggregate_key in batch_changed_keys:
                        aggregate = aggregated[aggregate_key]
                        self._upsert_term_candidate(
                            term=aggregate["term"],
                            normalized_term=aggregate["normalized_term"],
                            avg_confidence=aggregate["confidence_sum"] / max(aggregate["count"], 1),
                            evidence_items=aggregate["evidence_items"],
                            embedding_client=term_embedding_client,
                            embedding_model=term_embedding_model,
                            embedding_cache=term_embedding_cache,
                            existing_candidate_vectors=existing_candidate_vectors,
                        )
                    self.db.commit()
                    self._append_run_log(
                        run_id=run.id,
                        level="INFO",
                        message=(
                            f"Batch candidates upserted. range={start + 1}-{min(start + len(batch), len(sample_items))}, "
                            f"changed_candidate_groups={len(batch_changed_keys)}, total_candidate_groups={len(aggregated)}"
                        ),
                    )
                    self.db.commit()
                    logger.info(
                        "AI vocabulary run batch candidates upserted: run_id=%s batch_start=%s batch_end=%s changed_candidate_groups=%s total_candidate_groups=%s",
                        run.id,
                        start + 1,
                        min(start + len(batch), len(sample_items)),
                        len(batch_changed_keys),
                        len(aggregated),
                    )

                run.last_heartbeat_at = datetime.now(timezone.utc)
                run.last_progress_message = (
                    f"Processed batch ending at sample {run.processed_samples}/{len(sample_items)}; "
                    f"total_terms={total_terms}; candidate_groups={len(aggregated)}"
                )
                self._append_run_log(
                    run_id=run.id,
                    level="INFO",
                    message=(
                        f"Batch completed. processed={run.processed_samples}/{len(sample_items)}, "
                        f"total_terms={total_terms}, candidate_groups={len(aggregated)}"
                    ),
                )
                self.db.commit()
                logger.info(
                    "AI vocabulary run batch completed: run_id=%s processed=%s/%s accumulated_terms=%s candidate_groups=%s",
                    run.id,
                    run.processed_samples,
                    len(sample_items),
                    total_terms,
                    len(aggregated),
                )

            run.total_terms = total_terms
            run.status = "COMPLETED"
            run.finished_at = datetime.now(timezone.utc)
            run.last_heartbeat_at = run.finished_at
            run.last_progress_message = (
                f"Completed run with {run.processed_samples}/{len(sample_items)} samples and "
                f"{run.total_terms} valid terms"
            )
            self._append_run_log(
                run_id=run.id,
                level="INFO",
                message=(
                    f"Run completed. processed={run.processed_samples}/{len(sample_items)}, "
                    f"total_terms={run.total_terms}, candidate_groups={len(aggregated)}"
                ),
            )
            self.db.commit()
            logger.info(
                "AI vocabulary run completed: run_id=%s run_key=%s processed=%s total_terms=%s candidate_groups=%s finished_at=%s",
                run.id,
                run.run_key,
                run.processed_samples,
                run.total_terms,
                len(aggregated),
                run.finished_at.isoformat() if run.finished_at else None,
            )
            self.db.refresh(run)
            return run
        except Exception as exc:
            self.db.rollback()
            persisted_run = self.db.get(AIVocabularyRun, run_id)
            if persisted_run is not None:
                persisted_run.status = "FAILED"
                persisted_run.finished_at = datetime.now(timezone.utc)
                persisted_run.last_heartbeat_at = persisted_run.finished_at
                persisted_run.last_progress_message = f"Failed: {exc}"
                self._append_run_log(
                    run_id=persisted_run.id,
                    level="ERROR",
                    message=f"Run failed. error={exc}",
                )
                self.db.commit()
            logger.exception(
                "AI vocabulary run failed: run_id=%s error=%s",
                run_id,
                exc,
            )
            raise

    def _execute_batch(
        self,
        client,
        prompt_version: AIVocabularyPromptVersion,
        items: list[DatasetSampleItem],
    ) -> dict[str, tuple[list[dict[str, Any]], str]]:
        prompt = self._build_batch_prompt(prompt_version, items)
        raw_output = client.complete(prompt)
        parsed = self._parse_batch_model_output(raw_output)
        results: dict[str, tuple[list[dict[str, Any]], str]] = {}
        for item in items:
            item_terms = parsed.get(item.id, [])
            results[item.id] = (item_terms, raw_output)
        return results

    def _execute_single_item(
        self,
        client,
        prompt_version: AIVocabularyPromptVersion,
        item: DatasetSampleItem,
    ) -> tuple[list[dict[str, Any]], str]:
        prompt = self._build_extraction_prompt(prompt_version, item.sample_content)
        raw_output = client.complete(prompt)
        return self._parse_model_output(raw_output), raw_output

    def _initialize_llm_client(self, provider: str, model_name: str):
        provider_name = (provider or "openai").lower()
        if provider_name == "openai":
            return OpenAILLMClient(Config.OPENAI_API_KEY, model=model_name or Config.OPENAI_MODEL or "gpt-4o-mini")
        if provider_name == "ali":
            return AliyunLLMClient(Config.ALI_QWEN_API_KEY, model=model_name or Config.ALI_QWEN_MODEL or "qwen-plus")
        if provider_name == "google":
            return GoogleLLMClient(Config.GOOGLE_API_KEY, model=model_name or "text-bison-001")
        raise ValueError(f"Unsupported provider: {provider}")

    def _initialize_embedding_client(self, provider: str):
        provider_name = (provider or "openai").lower()
        if provider_name == "openai" and Config.OPENAI_API_KEY:
            return OpenAIEmbeddingClient(Config.OPENAI_API_KEY)
        if provider_name == "ali" and Config.ALI_QWEN_API_KEY:
            return AliEmbeddingClient(Config.ALI_QWEN_API_KEY)
        if provider_name == "google" and Config.GOOGLE_API_KEY:
            return GoogleEmbeddingClient(Config.GOOGLE_API_KEY)
        if Config.ALI_QWEN_API_KEY:
            return AliEmbeddingClient(Config.ALI_QWEN_API_KEY)
        if Config.OPENAI_API_KEY:
            return OpenAIEmbeddingClient(Config.OPENAI_API_KEY)
        return None

    def _embedding_model_for_provider(self, provider: str) -> str | None:
        provider_name = (provider or "openai").lower()
        if provider_name == "openai":
            return Config.OPENAI_EMBEDDING_MODEL or "text-embedding-3-small"
        if provider_name == "ali":
            return Config.ALI_EMBEDDING_MODEL or "text-embedding-v4"
        if provider_name == "google":
            return getattr(Config, "GOOGLE_EMBEDDING_MODEL", None) or "textembedding-gecko"
        return Config.ALI_EMBEDDING_MODEL or Config.OPENAI_EMBEDDING_MODEL or "text-embedding-3-small"

    def _build_extraction_prompt(self, prompt_version: AIVocabularyPromptVersion, sample_content: str) -> str:
        system_prompt = (prompt_version.system_prompt or "").strip()
        user_prompt = (prompt_version.user_prompt_template or "").format(sample_content=sample_content)
        if system_prompt:
            return f"{system_prompt}\n\n{user_prompt}"
        return user_prompt

    def _build_batch_prompt(
        self,
        prompt_version: AIVocabularyPromptVersion,
        items: list[DatasetSampleItem],
    ) -> str:
        docs_payload = [
            {
                "sample_item_id": item.id,
                "doc_id": item.doc_id,
                "chunk_id": item.chunk_id,
                "content": item.sample_content,
            }
            for item in items
        ]
        base = (prompt_version.system_prompt or "").strip()
        instructions = (
            "请基于下面多个样本分别抽取术语。"
            "必须按 sample_item_id 分组返回。"
            "输出 JSON 对象，格式为 "
            '{"items":[{"sample_item_id":"...","terms":[{"term":"术语","evidence":"原文证据","confidence":0.95}]}]}。'
            "如果某个样本没有合适术语，则 terms 返回 []。"
        )
        return f"{base}\n\n{instructions}\n\n样本如下：\n{json.dumps(docs_payload, ensure_ascii=False)}"

    def _parse_model_output(self, raw_output: str) -> list[dict[str, Any]]:
        text = (raw_output or "").strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            start = text.find("[")
            end = text.rfind("]")
            if start == -1 or end == -1 or end <= start:
                return []
            try:
                parsed = json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                return []

        if isinstance(parsed, dict):
            if isinstance(parsed.get("terms"), list):
                return [item for item in parsed["terms"] if isinstance(item, dict)]
            return []
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
        return []

    def _parse_batch_model_output(self, raw_output: str) -> dict[str, list[dict[str, Any]]]:
        text = (raw_output or "").strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            start = text.find("{")
            end = text.rfind("}")
            if start == -1 or end == -1 or end <= start:
                return {}
            try:
                parsed = json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                return {}

        if not isinstance(parsed, dict) or not isinstance(parsed.get("items"), list):
            return {}

        results: dict[str, list[dict[str, Any]]] = {}
        for item in parsed["items"]:
            if not isinstance(item, dict):
                continue
            sample_item_id = str(item.get("sample_item_id", "")).strip()
            terms = item.get("terms")
            if not sample_item_id or not isinstance(terms, list):
                continue
            results[sample_item_id] = [term for term in terms if isinstance(term, dict)]
        return results

    def _normalize_term(self, term: str) -> str:
        return " ".join((term or "").strip().split()).lower()

    def _build_run_key(
        self,
        dataset: str,
        sample_version_id: str,
        prompt_version: str,
        provider: str,
        model_name: str,
        temperature: float,
    ) -> str:
        payload = {
            "dataset": dataset,
            "sample_version_id": sample_version_id,
            "prompt_version": prompt_version,
            "provider": provider or "openai",
            "model_name": model_name or "",
            "temperature": round(float(temperature), 4),
        }
        encoded = json.dumps(payload, sort_keys=True, ensure_ascii=True)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:32]

    def _normalize_confidence(self, value: Any) -> float:
        try:
            confidence = float(value)
        except (TypeError, ValueError):
            confidence = 0.5
        return max(0.0, min(confidence, 1.0))

    def _append_run_log(self, run_id: str, level: str, message: str) -> None:
        self.db.add(
            AIVocabularyRunLog(
                ai_run_id=run_id,
                level=(level or "INFO").upper(),
                message=message,
            )
        )

    def _validate_term(
        self,
        sample_content: str,
        term: str,
        evidence: str,
        raw_model_output: str,
        confidence: float | None = None,
    ) -> str:
        if not term or not evidence:
            return "INVALID_JSON"
        if self._is_placeholder_term(term):
            return "FILTERED_NOISE"
        if not self._raw_output_supports_extraction(raw_model_output, term, evidence):
            return "INVALID_JSON"
        if evidence not in sample_content:
            return "INVALID_EVIDENCE"
        if term not in evidence and term not in sample_content:
            return "INVALID_EVIDENCE"
        if len(term) < 2:
            return "FILTERED_NOISE"
        if confidence is not None and confidence < LOW_CONFIDENCE_THRESHOLD:
            return "LOW_CONFIDENCE"
        return "VALID"

    def _is_placeholder_term(self, term: str) -> bool:
        value = (term or "").strip()
        if not value:
            return True
        return bool(re.fullmatch(r"[?？]{2,}\d*", value))

    def _raw_output_supports_extraction(self, raw_model_output: str, term: str, evidence: str) -> bool:
        text = (raw_model_output or "").strip()
        if not text or text == "[]":
            return False
        return term in text and evidence in text

    def _find_evidence_span(self, sample_content: str, evidence: str) -> tuple[int | None, int | None]:
        if not sample_content or not evidence:
            return None, None
        start = sample_content.find(evidence)
        if start < 0:
            return None, None
        return start, start + len(evidence)

    def _resolve_semantic_group(
        self,
        aggregated: dict[str, dict[str, Any]],
        term: str,
        normalized_term: str,
        embedding_client,
        embedding_model: str | None,
        embedding_cache: dict[str, list[float]],
    ) -> str:
        if normalized_term in aggregated:
            return normalized_term
        if embedding_client is None:
            return normalized_term

        term_vector = self._embed_term(term, embedding_client, embedding_model, embedding_cache)
        if not term_vector:
            return normalized_term

        best_key = normalized_term
        best_similarity = 0.0
        for aggregate_key, aggregate in aggregated.items():
            aggregate_term = aggregate.get("term") or aggregate_key
            aggregate_vector = self._embed_term(
                str(aggregate_term),
                embedding_client,
                embedding_model,
                embedding_cache,
            )
            similarity = _cosine_similarity(term_vector, aggregate_vector)
            if similarity > 0.90 and similarity > best_similarity:
                best_similarity = similarity
                best_key = aggregate_key
        return best_key

    def _embed_term(
        self,
        term: str,
        embedding_client,
        embedding_model: str | None,
        embedding_cache: dict[str, list[float]],
    ) -> list[float]:
        cache_key = self._normalize_term(term)
        if not cache_key:
            return []
        if cache_key in embedding_cache:
            return embedding_cache[cache_key]
        try:
            vector = embedding_client.embed(term, model=embedding_model)
        except Exception:
            vector = []
        embedding_cache[cache_key] = [float(item) for item in vector] if vector else []
        return embedding_cache[cache_key]

    def _clear_run_term_candidates(self, run_id: str) -> None:
        candidates = list(
            self.db.scalars(
                select(TermCandidate).where(TermCandidate.source.ilike("%ai_extract%"))
            ).all()
        )
        for candidate in candidates:
            retained_evidence = [
                item
                for item in self._normalize_evidence(candidate.evidence)
                if not (item.get("source") == "ai_extract" and item.get("ai_run_id") == run_id)
            ]
            if len(retained_evidence) == len(self._normalize_evidence(candidate.evidence)):
                continue

            if retained_evidence:
                candidate.evidence = retained_evidence
                candidate.source = self._derive_source_from_evidence(retained_evidence, fallback_source=candidate.source)
                candidate.confidence = self._compute_candidate_confidence(retained_evidence, candidate.confidence)
            elif self._has_non_ai_source(candidate.source):
                candidate.evidence = retained_evidence
                candidate.source = self._remove_source(candidate.source, "ai_extract")
                candidate.confidence = candidate.confidence
            else:
                self.db.delete(candidate)

    def _upsert_term_candidate(
        self,
        term: str,
        normalized_term: str,
        avg_confidence: float,
        evidence_items: list[dict[str, Any]],
        embedding_client,
        embedding_model: str | None,
        embedding_cache: dict[str, list[float]],
        existing_candidate_vectors: list[dict[str, Any]],
    ) -> None:
        stmt = select(TermCandidate).where(func.lower(TermCandidate.canonical) == normalized_term)
        candidate = self.db.scalar(stmt)
        if candidate is None:
            candidate = self._find_semantic_term_candidate(
                term=term,
                embedding_client=embedding_client,
                embedding_model=embedding_model,
                embedding_cache=embedding_cache,
                existing_candidate_vectors=existing_candidate_vectors,
            )
        if candidate is None:
            candidate = TermCandidate(
                canonical=term,
                aliases=[],
                role=None,
                definition=None,
                status="CANDIDATE",
                confidence=avg_confidence,
                source="ai_extract",
                owner="ai_vocabulary",
                topics=[],
                version=1,
                submitted_by="ai_vocabulary",
                evidence=self._dedupe_evidence(evidence_items),
                created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                updated_at=datetime.now(timezone.utc).replace(tzinfo=None),
            )
            self.db.add(candidate)
            self.db.flush()
            vector = self._embed_term(term, embedding_client, embedding_model, embedding_cache)
            if vector:
                existing_candidate_vectors.append({"candidate": candidate, "vector": vector})
            return

        candidate.canonical = candidate.canonical or term
        candidate.aliases = self._merge_aliases(candidate.aliases, term, candidate.canonical)
        candidate.source = self._merge_source(candidate.source, "ai_extract")
        candidate.confidence = max(candidate.confidence or 0.0, avg_confidence)
        candidate.owner = candidate.owner or "ai_vocabulary"
        candidate.submitted_by = candidate.submitted_by or "ai_vocabulary"
        candidate.evidence = self._dedupe_evidence(self._normalize_evidence(candidate.evidence) + evidence_items)
        candidate.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)

    def _find_semantic_term_candidate(
        self,
        term: str,
        embedding_client,
        embedding_model: str | None,
        embedding_cache: dict[str, list[float]],
        existing_candidate_vectors: list[dict[str, Any]],
    ) -> TermCandidate | None:
        if embedding_client is None:
            return None
        term_vector = self._embed_term(term, embedding_client, embedding_model, embedding_cache)
        if not term_vector:
            return None

        best_candidate = None
        best_similarity = 0.0
        for candidate_info in existing_candidate_vectors:
            candidate = candidate_info.get("candidate")
            candidate_vector = candidate_info.get("vector") or []
            similarity = _cosine_similarity(term_vector, candidate_vector)
            if similarity > 0.90 and similarity > best_similarity:
                best_similarity = similarity
                best_candidate = candidate
        return best_candidate

    def _load_existing_candidate_vectors(
        self,
        embedding_client,
        embedding_model: str | None,
        embedding_cache: dict[str, list[float]],
    ) -> list[dict[str, Any]]:
        if embedding_client is None:
            return []
        candidates = list(
            self.db.scalars(
                select(TermCandidate).where(TermCandidate.source.ilike("%ai_extract%"))
            ).all()
        )
        payload: list[dict[str, Any]] = []
        for candidate in candidates:
            candidate_term = (candidate.canonical or "").strip()
            if not candidate_term:
                continue
            vector = self._embed_term(candidate_term, embedding_client, embedding_model, embedding_cache)
            if vector:
                payload.append({"candidate": candidate, "vector": vector})
        return payload

    def _merge_aliases(self, aliases: Any, term: str, canonical: str) -> list[str]:
        values = [str(item).strip() for item in (aliases or []) if str(item).strip()]
        normalized_canonical = self._normalize_term(canonical)
        normalized_term = self._normalize_term(term)
        if normalized_term and normalized_term != normalized_canonical and term not in values:
            values.append(term)
        return values

    def _normalize_evidence(self, evidence: Any) -> list[dict[str, Any]]:
        if isinstance(evidence, list):
            return [item for item in evidence if isinstance(item, dict)]
        return []

    def _dedupe_evidence(self, evidence_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str, str]] = set()
        for item in evidence_items:
            key = (
                str(item.get("source", "")),
                str(item.get("ai_run_id", "")),
                str(item.get("doc_id", "")),
                str(item.get("evidence", "")),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _merge_source(self, current_source: str | None, new_source: str) -> str:
        sources = [part.strip() for part in (current_source or "").split(",") if part.strip()]
        if new_source not in sources:
            sources.append(new_source)
        return ",".join(sources) if sources else new_source

    def _remove_source(self, current_source: str | None, source_to_remove: str) -> str | None:
        sources = [part.strip() for part in (current_source or "").split(",") if part.strip()]
        retained = [part for part in sources if part != source_to_remove]
        return ",".join(retained) if retained else None

    def _has_non_ai_source(self, source: str | None) -> bool:
        sources = [part.strip() for part in (source or "").split(",") if part.strip()]
        return any(part != "ai_extract" for part in sources)

    def _derive_source_from_evidence(
        self,
        evidence_items: list[dict[str, Any]],
        fallback_source: str | None,
    ) -> str | None:
        derived_sources = [
            str(item.get("source", "")).strip()
            for item in evidence_items
            if str(item.get("source", "")).strip()
        ]
        if derived_sources:
            ordered = list(dict.fromkeys(derived_sources))
            return ",".join(ordered)
        return fallback_source

    def _compute_candidate_confidence(
        self,
        evidence_items: list[dict[str, Any]],
        fallback_confidence: float | None,
    ) -> float | None:
        confidences = []
        for item in evidence_items:
            try:
                confidences.append(float(item.get("confidence")))
            except (TypeError, ValueError):
                continue
        if confidences:
            return sum(confidences) / len(confidences)
        return fallback_confidence


class TermCandidateService:
    def __init__(self, db: Session):
        self.db = db

    def list_candidates(
        self,
        dataset: str | None = None,
        ai_run_id: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        stmt = select(TermCandidate).where(TermCandidate.source.ilike("%ai_extract%")).order_by(TermCandidate.updated_at.desc())
        if ai_run_id:
            pass
        if status:
            stmt = stmt.where(TermCandidate.status == status)
        candidates = list(self.db.scalars(stmt).all())
        serialized = [self._serialize_candidate(candidate) for candidate in candidates]
        if dataset:
            serialized = [
                candidate
                for candidate in serialized
                if dataset in candidate.get("datasets", [])
            ]
        if ai_run_id:
            serialized = [
                candidate
                for candidate in serialized
                if ai_run_id in candidate.get("ai_run_ids", [])
            ]
        return serialized

    def update_candidate_status(self, candidate_id: int, status: str) -> TermCandidate:
        normalized_status = (status or "").strip().upper()
        if normalized_status == "APPROVED":
            normalized_status = "PUBLISHED"
        if normalized_status not in {"CANDIDATE", "PUBLISHED", "REJECTED"}:
            raise ValueError(f"Unsupported candidate status: {status}")

        candidate = self.db.get(TermCandidate, candidate_id)
        if candidate is None:
            raise ValueError(f"Candidate not found: {candidate_id}")

        candidate.status = normalized_status
        candidate.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
        if normalized_status == "PUBLISHED":
            candidate.published_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self.db.commit()
        self.db.refresh(candidate)
        return candidate

    def get_candidate(self, candidate_id: int) -> dict[str, Any]:
        candidate = self.db.get(TermCandidate, candidate_id)
        if candidate is None:
            raise ValueError(f"Candidate not found: {candidate_id}")
        return self._serialize_candidate(candidate)

    def list_candidate_evidence(self, candidate_id: int) -> list[dict[str, Any]]:
        candidate = self.db.get(TermCandidate, candidate_id)
        if candidate is None:
            raise ValueError(f"Candidate not found: {candidate_id}")

        rows = [
            item
            for item in self._normalize_evidence(candidate.evidence)
            if self._is_ai_vocabulary_evidence(item)
        ]
        return [
            {
                "raw_term_id": item.get("raw_term_id"),
                "doc_id": item.get("doc_id"),
                "chunk_id": item.get("chunk_id"),
                "term": candidate.canonical,
                "evidence": item.get("evidence", ""),
                "confidence": float(item.get("confidence", 0.0) or 0.0),
                "validation_status": "VALID",
                "created_at": self._parse_datetime(item.get("created_at")),
            }
            for item in rows
        ]

    def _serialize_candidate(self, candidate: TermCandidate) -> dict[str, Any]:
        ai_evidence = [
            item
            for item in self._normalize_evidence(candidate.evidence)
            if self._is_ai_vocabulary_evidence(item)
        ]
        ai_evidence = sorted(
            ai_evidence,
            key=lambda item: (
                self._parse_datetime(item.get("created_at")),
                float(item.get("confidence", 0.0) or 0.0),
            ),
            reverse=True,
        )
        evidence_count = len(ai_evidence)
        document_count = len({str(item.get("doc_id", "")).strip() for item in ai_evidence if item.get("doc_id")})
        representative = ai_evidence[0] if ai_evidence else {}
        dataset = representative.get("dataset")
        ai_run_id = representative.get("ai_run_id")
        sample_version_id = representative.get("sample_version_id")
        doc_id = representative.get("doc_id")
        evidence = ""
        if ai_evidence:
            evidence = max((str(item.get("evidence", "")) for item in ai_evidence), key=len, default="")
        datasets = list(dict.fromkeys(str(item.get("dataset")) for item in ai_evidence if item.get("dataset")))
        ai_run_ids = list(dict.fromkeys(str(item.get("ai_run_id")) for item in ai_evidence if item.get("ai_run_id")))

        return {
            "id": candidate.id,
            "dataset": dataset,
            "datasets": datasets,
            "term": candidate.canonical,
            "normalized_term": " ".join((candidate.canonical or "").strip().split()).lower(),
            "source": candidate.source or "ai_extract",
            "ai_run_id": ai_run_id,
            "ai_run_ids": ai_run_ids,
            "sample_version_id": sample_version_id,
            "doc_id": doc_id,
            "evidence": evidence,
            "confidence": candidate.confidence or 0.0,
            "evidence_count": evidence_count,
            "document_count": document_count,
            "status": candidate.status,
            "reviewed": candidate.status != "CANDIDATE",
            "created_at": candidate.created_at,
            "updated_at": candidate.updated_at,
        }

    def _normalize_evidence(self, evidence: Any) -> list[dict[str, Any]]:
        if isinstance(evidence, list):
            return [item for item in evidence if isinstance(item, dict)]
        return []

    def _is_ai_vocabulary_evidence(self, item: dict[str, Any]) -> bool:
        source = str(item.get("source", "")).strip()
        return source in {"ai_extract", "AI_RUN_DETAIL"} or bool(item.get("ai_run_id")) or bool(item.get("raw_term_id"))

    def _parse_datetime(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                pass
        return datetime.now(timezone.utc)


class RawTermGovernanceService:
    def __init__(self, db: Session):
        self.db = db

    def add_to_candidate(
        self,
        raw_term_id: str,
        term: str,
        normalized_term: str | None = None,
        source: str = "AI_RUN_DETAIL",
    ) -> dict[str, Any]:
        raw_term = self._get_raw_term(raw_term_id)
        normalized = normalized_term or self._normalize_term(term)
        if not term.strip():
            raise ValueError("term is required")

        linkage = RunAnalyticsService(self.db)._get_run_candidate_linkage(raw_term.ai_run_id)
        existing_candidate_id = linkage.get(raw_term.id)
        if existing_candidate_id is not None:
            candidate = self.db.get(TermCandidate, existing_candidate_id)
            if candidate is None:
                raise ValueError(f"Candidate not found: {existing_candidate_id}")
            return {
                "candidateId": candidate.id,
                "term": candidate.canonical,
                "status": candidate.status,
                "createdAt": candidate.created_at,
            }

        evidence_item = {
            "source": source or "AI_RUN_DETAIL",
            "dataset": raw_term.dataset,
            "ai_run_id": raw_term.ai_run_id,
            "sample_item_id": raw_term.sample_item_id,
            "raw_term_id": raw_term.id,
            "doc_id": raw_term.doc_id,
            "chunk_id": raw_term.chunk_id,
            "evidence": raw_term.evidence,
            "confidence": raw_term.confidence,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "action": "manual_add_candidate",
        }
        candidate = self.db.scalar(
            select(TermCandidate).where(func.lower(TermCandidate.canonical) == normalized)
        )
        if candidate is None:
            candidate = TermCandidate(
                canonical=term,
                aliases=[],
                role=None,
                definition=None,
                status="CANDIDATE",
                confidence=raw_term.confidence,
                source="ai_extract,AI_RUN_DETAIL",
                owner="ai_vocabulary",
                topics=[],
                version=1,
                submitted_by="ai_vocabulary",
                evidence=[evidence_item],
                created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                updated_at=datetime.now(timezone.utc).replace(tzinfo=None),
            )
            self.db.add(candidate)
            self.db.commit()
            self.db.refresh(candidate)
            return {
                "candidateId": candidate.id,
                "term": candidate.canonical,
                "status": candidate.status,
                "createdAt": candidate.created_at,
            }

        evidence_items = TermCandidateService(self.db)._normalize_evidence(candidate.evidence)
        exists = any(str(item.get("raw_term_id", "")).strip() == raw_term.id for item in evidence_items)
        if not exists:
            evidence_items.append(evidence_item)
        candidate.aliases = AIVocabularyRunService(self.db)._merge_aliases(candidate.aliases, term, candidate.canonical)
        candidate.source = AIVocabularyRunService(self.db)._merge_source(candidate.source, "ai_extract")
        candidate.source = AIVocabularyRunService(self.db)._merge_source(candidate.source, source or "AI_RUN_DETAIL")
        candidate.confidence = max(candidate.confidence or 0.0, raw_term.confidence)
        candidate.evidence = AIVocabularyRunService(self.db)._dedupe_evidence(evidence_items)
        candidate.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self.db.commit()
        self.db.refresh(candidate)
        return {
            "candidateId": candidate.id,
            "term": candidate.canonical,
            "status": candidate.status,
            "createdAt": candidate.created_at,
        }

    def ignore_raw_term(self, raw_term_id: str, reason: str, note: str | None = None) -> dict[str, Any]:
        raw_term = self._get_raw_term(raw_term_id)
        raw_term.ignored_at = datetime.now(timezone.utc)
        raw_term.ignore_reason = (reason or "FILTERED_NOISE").strip().upper()
        raw_term.ignore_note = note
        raw_term.validation_status = raw_term.ignore_reason
        self.db.commit()
        self.db.refresh(raw_term)
        return {
            "rawTermId": raw_term.id,
            "ignored": True,
            "ignoredAt": raw_term.ignored_at,
            "validationStatus": raw_term.validation_status,
        }

    def unignore_raw_term(self, raw_term_id: str) -> dict[str, Any]:
        raw_term = self._get_raw_term(raw_term_id)
        raw_term.ignored_at = None
        raw_term.ignore_reason = None
        raw_term.ignore_note = None
        raw_term.validation_status = self._recompute_validation_status(raw_term)
        self.db.commit()
        self.db.refresh(raw_term)
        return {
            "rawTermId": raw_term.id,
            "ignored": False,
            "ignoredAt": None,
            "validationStatus": raw_term.validation_status,
        }

    def _get_raw_term(self, raw_term_id: str) -> AIVocabularyTermRaw:
        raw_term = self.db.get(AIVocabularyTermRaw, raw_term_id)
        if raw_term is None:
            raise ValueError(f"Raw term not found: {raw_term_id}")
        return raw_term

    def _normalize_term(self, term: str) -> str:
        return " ".join((term or "").strip().split()).lower()

    def _recompute_validation_status(self, raw_term: AIVocabularyTermRaw) -> str:
        sample_item = self.db.get(DatasetSampleItem, raw_term.sample_item_id)
        sample_content = sample_item.sample_content if sample_item is not None else raw_term.evidence
        return AIVocabularyRunService(self.db)._validate_term(
            sample_content=sample_content,
            term=raw_term.term,
            evidence=raw_term.evidence,
            raw_model_output=raw_term.raw_model_output,
            confidence=raw_term.confidence,
        )


class RunAnalyticsService:
    def __init__(self, db: Session):
        self.db = db

    def get_run_summary(self, run_id: str) -> dict[str, Any]:
        run = self.db.get(AIVocabularyRun, run_id)
        if run is None:
            raise ValueError(f"Run not found: {run_id}")
        sample_version = self.db.get(DatasetSampleVersion, run.sample_version_id)
        raw_terms = list(
            self.db.scalars(
                select(AIVocabularyTermRaw).where(
                    AIVocabularyTermRaw.ai_run_id == run_id,
                    AIVocabularyTermRaw.ignored_at.is_(None),
                )
            ).all()
        )
        breakdown = self._compute_breakdown(raw_terms)
        raw_term_count = len(raw_terms)
        valid_term_count = breakdown.get("VALID", 0)
        invalid_term_count = raw_term_count - valid_term_count
        active_raw_term_ids = {row.id for row in raw_terms}
        linkage = self._get_run_candidate_linkage(run_id)
        candidate_count = len({candidate_id for raw_term_id, candidate_id in linkage.items() if raw_term_id in active_raw_term_ids})

        return {
            "runId": run.id,
            "dataset": run.dataset,
            "sampleVersion": sample_version.version_name if sample_version is not None else run.sample_version_id,
            "promptVersion": run.prompt_version,
            "model": run.model_name or run.provider,
            "status": run.status,
            "createdAt": run.created_at,
            "startedAt": run.started_at,
            "finishedAt": run.finished_at,
            "metrics": {
                "totalSamples": run.total_samples,
                "rawTerms": raw_term_count,
                "validTerms": valid_term_count,
                "invalidTerms": invalid_term_count,
                "candidates": candidate_count,
                "validRate": self._safe_rate(valid_term_count, raw_term_count),
                "evidenceFailRate": self._safe_rate(breakdown.get("INVALID_EVIDENCE", 0), raw_term_count),
                "noiseRate": self._safe_rate(breakdown.get("FILTERED_NOISE", 0), raw_term_count),
            },
        }

    def get_invalid_breakdown(self, run_id: str) -> dict[str, Any]:
        self._require_run(run_id)
        raw_terms = list(
            self.db.scalars(
                select(AIVocabularyTermRaw).where(
                    AIVocabularyTermRaw.ai_run_id == run_id,
                    AIVocabularyTermRaw.ignored_at.is_(None),
                )
            ).all()
        )
        breakdown = self._compute_breakdown(raw_terms)
        ordered_types = ["INVALID_JSON", "INVALID_EVIDENCE", "FILTERED_NOISE", "LOW_CONFIDENCE"]
        return {
            "breakdown": [
                {"type": status, "count": breakdown.get(status, 0)}
                for status in ordered_types
                if breakdown.get(status, 0) > 0
            ]
        }

    def list_run_terms(
        self,
        run_id: str,
        validation_status: str | None = None,
        confidence_min: float | None = None,
        has_candidate: bool | None = None,
        doc_id: str | None = None,
        term: str | None = None,
        sort_by: str = "createdAt",
        sort_order: str = "desc",
        page: int = 1,
        size: int = 20,
    ) -> dict[str, Any]:
        self._require_run(run_id)
        linkage = self._get_run_candidate_linkage(run_id)
        linked_ids = set(linkage.keys())

        stmt = select(AIVocabularyTermRaw).where(
            AIVocabularyTermRaw.ai_run_id == run_id,
            AIVocabularyTermRaw.ignored_at.is_(None),
        )
        if validation_status:
            stmt = self._apply_validation_status_filter(stmt, validation_status)
        if confidence_min is not None:
            stmt = stmt.where(AIVocabularyTermRaw.confidence >= confidence_min)
        if doc_id:
            stmt = stmt.where(AIVocabularyTermRaw.doc_id == doc_id)
        if term:
            like_pattern = f"%{term.strip()}%"
            stmt = stmt.where(
                or_(
                    AIVocabularyTermRaw.term.ilike(like_pattern),
                    AIVocabularyTermRaw.normalized_term.ilike(like_pattern),
                )
            )
        if has_candidate is True:
            if not linked_ids:
                return {"total": 0, "items": []}
            stmt = stmt.where(AIVocabularyTermRaw.id.in_(linked_ids))
        elif has_candidate is False and linked_ids:
            stmt = stmt.where(AIVocabularyTermRaw.id.not_in(linked_ids))

        total = self.db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
        sort_column = RUN_TERM_SORT_FIELDS.get(sort_by, AIVocabularyTermRaw.created_at)
        sort_func = asc if (sort_order or "").lower() == "asc" else desc
        page = max(page, 1)
        size = max(1, min(size, 200))
        rows = list(
            self.db.scalars(
                stmt.order_by(sort_func(sort_column), AIVocabularyTermRaw.id.asc())
                .offset((page - 1) * size)
                .limit(size)
            ).all()
        )
        return {
            "total": total,
            "items": [self._serialize_run_term(row, linkage.get(row.id)) for row in rows],
        }

    def get_top_candidates(self, run_id: str, limit: int = 20) -> dict[str, Any]:
        self._require_run(run_id)
        items: list[dict[str, Any]] = []
        active_raw_term_ids = {row.id for row in self._load_active_run_terms(run_id)}
        candidates = list(
            self.db.scalars(
                select(TermCandidate).where(TermCandidate.source.ilike("%ai_extract%"))
            ).all()
        )
        for candidate in candidates:
            ai_evidence = [
                item
                for item in TermCandidateService(self.db)._normalize_evidence(candidate.evidence)
                if (
                    TermCandidateService(self.db)._is_ai_vocabulary_evidence(item)
                    and item.get("ai_run_id") == run_id
                    and str(item.get("raw_term_id", "")).strip() in active_raw_term_ids
                )
            ]
            if not ai_evidence:
                continue
            items.append(
                {
                    "term": candidate.canonical,
                    "candidateId": candidate.id,
                    "evidenceCount": len(ai_evidence),
                }
            )
        items.sort(key=lambda item: (-item["evidenceCount"], item["term"]))
        return {"items": items[: max(1, min(limit, 100))]}

    def compare_runs(self, run_id: str, target_run_id: str, top_n: int = 20) -> dict[str, Any]:
        if run_id == target_run_id:
            raise ValueError("targetRunId must be different from run_id")
        base_summary = self.get_run_summary(run_id)
        target_summary = self.get_run_summary(target_run_id)
        base_breakdown = self._compute_breakdown(self._load_active_run_terms(run_id))
        target_breakdown = self._compute_breakdown(self._load_active_run_terms(target_run_id))

        diff_types = ["INVALID_JSON", "INVALID_EVIDENCE", "FILTERED_NOISE", "LOW_CONFIDENCE"]
        invalid_breakdown_diff = [
            {
                "type": breakdown_type,
                "baseCount": base_breakdown.get(breakdown_type, 0),
                "targetCount": target_breakdown.get(breakdown_type, 0),
                "delta": target_breakdown.get(breakdown_type, 0) - base_breakdown.get(breakdown_type, 0),
            }
            for breakdown_type in diff_types
        ]

        return {
            "baseRun": base_summary,
            "targetRun": target_summary,
            "metricsDiff": {
                "rawTerms": target_summary["metrics"]["rawTerms"] - base_summary["metrics"]["rawTerms"],
                "validTerms": target_summary["metrics"]["validTerms"] - base_summary["metrics"]["validTerms"],
                "invalidTerms": target_summary["metrics"]["invalidTerms"] - base_summary["metrics"]["invalidTerms"],
                "candidates": target_summary["metrics"]["candidates"] - base_summary["metrics"]["candidates"],
                "validRate": round(
                    target_summary["metrics"]["validRate"] - base_summary["metrics"]["validRate"],
                    4,
                ),
                "evidenceFailRate": round(
                    target_summary["metrics"]["evidenceFailRate"] - base_summary["metrics"]["evidenceFailRate"],
                    4,
                ),
                "noiseRate": round(
                    target_summary["metrics"]["noiseRate"] - base_summary["metrics"]["noiseRate"],
                    4,
                ),
            },
            "invalidBreakdownDiff": invalid_breakdown_diff,
            "topTermChanges": self._compare_top_terms(run_id, target_run_id, limit=top_n),
        }

    def _require_run(self, run_id: str) -> AIVocabularyRun:
        run = self.db.get(AIVocabularyRun, run_id)
        if run is None:
            raise ValueError(f"Run not found: {run_id}")
        return run

    def _compute_breakdown(self, rows: list[AIVocabularyTermRaw]) -> dict[str, int]:
        breakdown = {
            "VALID": 0,
            "INVALID_JSON": 0,
            "INVALID_EVIDENCE": 0,
            "FILTERED_NOISE": 0,
            "LOW_CONFIDENCE": 0,
        }
        for row in rows:
            status = normalize_validation_status(row.validation_status, row.confidence)
            breakdown[status] = breakdown.get(status, 0) + 1
        return breakdown

    def _load_active_run_terms(self, run_id: str) -> list[AIVocabularyTermRaw]:
        return list(
            self.db.scalars(
                select(AIVocabularyTermRaw).where(
                    AIVocabularyTermRaw.ai_run_id == run_id,
                    AIVocabularyTermRaw.ignored_at.is_(None),
                )
            ).all()
        )

    def _safe_rate(self, numerator: int, denominator: int) -> float:
        if denominator <= 0:
            return 0.0
        return round(numerator / denominator, 4)

    def _statuses_for_filter(self, validation_status: str) -> set[str]:
        normalized = (validation_status or "").strip().upper()
        return VALIDATION_STATUS_COMPATIBILITY.get(normalized, {normalized})

    def _apply_validation_status_filter(self, stmt, validation_status: str):
        normalized = (validation_status or "").strip().upper()
        if normalized == "VALID":
            return stmt.where(
                AIVocabularyTermRaw.validation_status == "VALID",
                or_(
                    AIVocabularyTermRaw.confidence.is_(None),
                    AIVocabularyTermRaw.confidence >= LOW_CONFIDENCE_THRESHOLD,
                ),
            )
        if normalized == "LOW_CONFIDENCE":
            return stmt.where(
                or_(
                    AIVocabularyTermRaw.validation_status == "LOW_CONFIDENCE",
                    and_(
                        AIVocabularyTermRaw.validation_status == "VALID",
                        AIVocabularyTermRaw.confidence < LOW_CONFIDENCE_THRESHOLD,
                    ),
                )
            )
        return stmt.where(
            AIVocabularyTermRaw.validation_status.in_(self._statuses_for_filter(normalized))
        )

    def _get_run_candidate_linkage(self, run_id: str) -> dict[str, int]:
        linkage: dict[str, int] = {}
        candidates = list(
            self.db.scalars(
                select(TermCandidate).where(TermCandidate.source.ilike("%ai_extract%"))
            ).all()
        )
        for candidate in candidates:
            evidence_items = TermCandidateService(self.db)._normalize_evidence(candidate.evidence)
            for item in evidence_items:
                raw_term_id = str(item.get("raw_term_id", "")).strip()
                if (
                    item.get("ai_run_id") == run_id
                    and raw_term_id
                ):
                    linkage[raw_term_id] = candidate.id
        return linkage

    def _serialize_run_term(self, row: AIVocabularyTermRaw, candidate_id: int | None) -> dict[str, Any]:
        return {
            "rawTermId": row.id,
            "term": row.term,
            "normalizedTerm": row.normalized_term,
            "confidence": row.confidence,
            "validationStatus": normalize_validation_status(row.validation_status, row.confidence),
            "docId": row.doc_id,
            "chunkId": row.chunk_id,
            "evidence": row.evidence,
            "evidenceStart": row.evidence_start,
            "evidenceEnd": row.evidence_end,
            "hasCandidate": candidate_id is not None,
            "candidateId": candidate_id,
        }

    def _compare_top_terms(self, base_run_id: str, target_run_id: str, limit: int = 20) -> list[dict[str, Any]]:
        base_counts = self._valid_term_counts(base_run_id)
        target_counts = self._valid_term_counts(target_run_id)
        all_terms = set(base_counts) | set(target_counts)
        changes: list[dict[str, Any]] = []
        for term in all_terms:
            base_count = base_counts.get(term, 0)
            target_count = target_counts.get(term, 0)
            if base_count == target_count:
                continue
            if base_count == 0:
                change_type = "NEW"
            elif target_count == 0:
                change_type = "DROPPED"
            elif target_count > base_count:
                change_type = "UP"
            else:
                change_type = "DOWN"
            changes.append(
                {
                    "term": term,
                    "baseCount": base_count,
                    "targetCount": target_count,
                    "changeType": change_type,
                    "_abs_delta": abs(target_count - base_count),
                }
            )
        changes.sort(key=lambda item: (-item["_abs_delta"], item["term"]))
        return [{key: value for key, value in item.items() if key != "_abs_delta"} for item in changes[: max(1, min(limit, 100))]]

    def _valid_term_counts(self, run_id: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self._load_active_run_terms(run_id):
            if normalize_validation_status(row.validation_status, row.confidence) != "VALID":
                continue
            key = row.normalized_term or self._fallback_normalized_term(row.term)
            counts[key] = counts.get(key, 0) + 1
        return counts

    def _fallback_normalized_term(self, term: str) -> str:
        return " ".join((term or "").strip().split()).lower()
