from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.ai_vocabulary.models import AIVocabularyRun, DatasetSampleItem, DatasetSampleVersion
from app.ai_vocabulary.schemas import (
    AddRawTermCandidateRequest,
    AddRawTermCandidateResponse,
    CompareRunResponse,
    CreatePromptVersionRequest,
    CreateRunRequest,
    GenerateSampleRequest,
    InvalidBreakdownResponse,
    IgnoreRawTermRequest,
    IgnoreRawTermResponse,
    PromptVersionResponse,
    RerunRequest,
    ReviewCandidateRequest,
    RunDetailSummaryResponse,
    RunLogResponse,
    RunTermsPageResponse,
    RunTopCandidatesResponse,
    RunResponse,
    SampleItemResponse,
    SampleVersionResponse,
    TermCandidateEvidenceResponse,
    TermCandidateResponse,
)
from app.ai_vocabulary.services import (
    AIVocabularyRunService,
    PromptVersionService,
    RawTermGovernanceService,
    RunAnalyticsService,
    SampleGenerationService,
    TermCandidateService,
)
from app.db import get_db_session
from app.worker.tasks import execute_ai_vocabulary_run_task
from app.db.session import SessionLocal

router = APIRouter()
DbSession = Annotated[Session, Depends(get_db_session)]


@router.post("/samples/generate", response_model=SampleVersionResponse, summary="Generate frozen base sample")
def generate_sample(request: GenerateSampleRequest, db: DbSession):
    service = SampleGenerationService(db)
    try:
        return service.generate_base_sample(request)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to generate sample: {exc}") from exc


@router.get("/samples/versions", response_model=list[SampleVersionResponse], summary="List sample versions")
def list_sample_versions(db: DbSession, dataset: str | None = None):
    stmt = select(DatasetSampleVersion).order_by(desc(DatasetSampleVersion.created_at))
    if dataset:
        stmt = stmt.where(DatasetSampleVersion.dataset == dataset)
    return list(db.scalars(stmt).all())


@router.get("/samples/versions/{sample_version_id}", response_model=SampleVersionResponse, summary="Get sample version")
def get_sample_version(sample_version_id: str, db: DbSession):
    sample_version = db.get(DatasetSampleVersion, sample_version_id)
    if sample_version is None:
        raise HTTPException(status_code=404, detail="Sample version not found")
    return sample_version


@router.get(
    "/samples/versions/{sample_version_id}/items",
    response_model=list[SampleItemResponse],
    summary="List sample items",
)
def list_sample_items(sample_version_id: str, db: DbSession):
    stmt = select(DatasetSampleItem).where(
        DatasetSampleItem.sample_version_id == sample_version_id
    ).order_by(DatasetSampleItem.created_at.asc())
    return list(db.scalars(stmt).all())


@router.post("/runs", response_model=RunResponse, summary="Create AI vocabulary extraction run")
def create_run(request: CreateRunRequest, db: DbSession):
    service = AIVocabularyRunService(db)
    try:
        return service.create_run(request)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to create run: {exc}") from exc


@router.get("/runs", response_model=list[RunResponse], summary="List AI vocabulary extraction runs")
def list_runs(
    db: DbSession,
    dataset: str | None = None,
    status: str | None = None,
    limit: int = 20,
    offset: int = 0,
):
    service = AIVocabularyRunService(db)
    return service.list_runs(dataset=dataset, status=status, limit=limit, offset=offset)


@router.get("/runs/{run_id}", response_model=RunResponse, summary="Get AI vocabulary extraction run")
def get_run(run_id: str, db: DbSession):
    run = db.get(AIVocabularyRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


@router.post("/runs/{run_id}/execute", response_model=RunResponse, summary="Execute AI vocabulary extraction run")
def execute_run(run_id: str, db: DbSession):
    service = AIVocabularyRunService(db)
    try:
        return service.execute_run(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to execute run: {exc}") from exc


@router.post("/runs/{run_id}/execute-async", summary="Queue AI vocabulary extraction run")
def execute_run_async(run_id: str, background_tasks: BackgroundTasks, db: DbSession):
    run = db.get(AIVocabularyRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    try:
        if hasattr(execute_ai_vocabulary_run_task, "delay"):
            task = execute_ai_vocabulary_run_task.delay(run_id)
            return {"status": "queued", "run_id": run_id, "task_id": getattr(task, "id", None), "mode": "celery"}

        def _execute_in_background(target_run_id: str):
            task_db = SessionLocal()
            try:
                service = AIVocabularyRunService(task_db)
                service.execute_run(target_run_id)
            finally:
                task_db.close()

        background_tasks.add_task(_execute_in_background, run_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to queue run: {exc}") from exc
    return {"status": "queued", "run_id": run_id, "task_id": None, "mode": "background-task"}


@router.get("/runs/{run_id}/terms", response_model=RunTermsPageResponse, summary="List run terms with filters")
def list_run_terms(
    run_id: str,
    db: DbSession,
    validationStatus: str | None = None,
    confidenceMin: float | None = None,
    hasCandidate: bool | None = None,
    docId: str | None = None,
    term: str | None = None,
    sortBy: str = "createdAt",
    sortOrder: str = "desc",
    page: int = 1,
    size: int = 20,
):
    service = RunAnalyticsService(db)
    try:
        return service.list_run_terms(
            run_id=run_id,
            validation_status=validationStatus,
            confidence_min=confidenceMin,
            has_candidate=hasCandidate,
            doc_id=docId,
            term=term,
            sort_by=sortBy,
            sort_order=sortOrder,
            page=page,
            size=size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/candidates", response_model=list[TermCandidateResponse], summary="List AI vocabulary candidates")
def list_candidates(
    db: DbSession,
    dataset: str | None = None,
    ai_run_id: str | None = None,
    status: str | None = None,
):
    service = TermCandidateService(db)
    return service.list_candidates(dataset=dataset, ai_run_id=ai_run_id, status=status)


@router.get(
    "/candidates/{candidate_id}",
    response_model=TermCandidateResponse,
    summary="Get candidate vocabulary detail",
)
def get_candidate(candidate_id: int, db: DbSession):
    service = TermCandidateService(db)
    try:
        return service.get_candidate(candidate_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/candidates/{candidate_id}/evidence",
    response_model=list[TermCandidateEvidenceResponse],
    summary="List candidate evidence rows",
)
def list_candidate_evidence(candidate_id: int, db: DbSession):
    service = TermCandidateService(db)
    try:
        return service.list_candidate_evidence(candidate_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/candidates/{candidate_id}/review",
    response_model=TermCandidateResponse,
    summary="Review candidate vocabulary term",
)
def review_candidate(candidate_id: int, request: ReviewCandidateRequest, db: DbSession):
    service = TermCandidateService(db)
    try:
        return service.update_candidate_status(candidate_id, request.status)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc


@router.get("/prompts", response_model=list[PromptVersionResponse], summary="List prompt versions")
def list_prompt_versions(db: DbSession):
    service = PromptVersionService(db)
    return service.list_prompt_versions()


@router.get("/prompts/{prompt_version}", response_model=PromptVersionResponse, summary="Get prompt version")
def get_prompt_version(prompt_version: str, db: DbSession):
    service = PromptVersionService(db)
    try:
        return service.get_prompt_version(prompt_version)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/prompts", response_model=PromptVersionResponse, summary="Create prompt version")
def create_prompt_version(request: CreatePromptVersionRequest, db: DbSession):
    service = PromptVersionService(db)
    try:
        return service.create_prompt_version(
            prompt_version=request.prompt_version,
            name=request.name,
            description=request.description,
            system_prompt=request.system_prompt,
            user_prompt_template=request.user_prompt_template,
            is_active=request.is_active,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/runs/{run_id}/summary", response_model=RunDetailSummaryResponse, summary="Get run analytics summary")
def get_run_summary(run_id: str, db: DbSession):
    service = RunAnalyticsService(db)
    try:
        return service.get_run_summary(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/runs/{run_id}/invalid-breakdown",
    response_model=InvalidBreakdownResponse,
    summary="Get invalid term breakdown",
)
def get_invalid_breakdown(run_id: str, db: DbSession):
    service = RunAnalyticsService(db)
    try:
        return service.get_invalid_breakdown(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/runs/{run_id}/top-candidates",
    response_model=RunTopCandidatesResponse,
    summary="Get top linked candidates for a run",
)
def get_top_candidates(run_id: str, db: DbSession, limit: int = 20):
    service = RunAnalyticsService(db)
    try:
        return service.get_top_candidates(run_id=run_id, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs/{run_id}/compare", response_model=CompareRunResponse, summary="Compare two runs")
def compare_runs(run_id: str, targetRunId: str, db: DbSession, topN: int = 20):
    service = RunAnalyticsService(db)
    try:
        return service.compare_runs(run_id=run_id, target_run_id=targetRunId, top_n=topN)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc


@router.post("/runs/{run_id}/rerun", response_model=RunResponse, summary="Create a rerun from an existing run")
def rerun(run_id: str, request: RerunRequest, db: DbSession):
    service = AIVocabularyRunService(db)
    try:
        return service.rerun(
            source_run_id=run_id,
            prompt_version=request.promptVersion,
            temperature=request.temperature,
            provider=request.provider,
            model_name=request.modelName,
            batch_size=request.batchSize,
        )
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc


@router.post(
    "/raw-terms/{raw_term_id}/candidate",
    response_model=AddRawTermCandidateResponse,
    summary="Push a raw term into candidate governance",
)
def add_raw_term_to_candidate(raw_term_id: str, request: AddRawTermCandidateRequest, db: DbSession):
    service = RawTermGovernanceService(db)
    try:
        return service.add_to_candidate(
            raw_term_id=raw_term_id,
            term=request.term,
            normalized_term=request.normalizedTerm,
            source=request.source,
        )
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc


@router.post(
    "/raw-terms/{raw_term_id}/ignore",
    response_model=IgnoreRawTermResponse,
    summary="Ignore a raw term from run detail tuning views",
)
def ignore_raw_term(raw_term_id: str, request: IgnoreRawTermRequest, db: DbSession):
    service = RawTermGovernanceService(db)
    try:
        return service.ignore_raw_term(raw_term_id=raw_term_id, reason=request.reason, note=request.note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/raw-terms/{raw_term_id}/unignore",
    response_model=IgnoreRawTermResponse,
    summary="Restore a previously ignored raw term",
)
def unignore_raw_term(raw_term_id: str, db: DbSession):
    service = RawTermGovernanceService(db)
    try:
        return service.unignore_raw_term(raw_term_id=raw_term_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs/{run_id}/logs", response_model=list[RunLogResponse], summary="List run execution logs")
def list_run_logs(run_id: str, db: DbSession, limit: int = 200):
    run = db.get(AIVocabularyRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    service = AIVocabularyRunService(db)
    return service.list_run_logs(run_id=run_id, limit=limit)
