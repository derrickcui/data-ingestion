from app.db.base import Base
from app.db.session import engine
from sqlalchemy import inspect, text
from app.utility.config import Config
from app.utility.log import logger


def init_database() -> None:
    if not Config.DATABASE_AUTO_CREATE:
        logger.info("Database auto-create is disabled.")
        return

    from app.ai_vocabulary import models  # noqa: F401
    from app.ai_vocabulary.services import PromptVersionService
    from app.db.session import SessionLocal

    Base.metadata.create_all(bind=engine)
    _run_lightweight_migrations()
    db = SessionLocal()
    try:
        PromptVersionService(db).ensure_default_prompt()
    finally:
        db.close()
    logger.info("Database schema initialized.")


def _run_lightweight_migrations() -> None:
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())

    if "ai_vocabulary_runs" in existing_tables:
        existing_columns = {column["name"] for column in inspector.get_columns("ai_vocabulary_runs")}
        with engine.begin() as conn:
            if "batch_size" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE ai_vocabulary_runs ADD COLUMN batch_size INTEGER DEFAULT 10")
                )
            if "run_key" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE ai_vocabulary_runs ADD COLUMN run_key VARCHAR(128)")
                )
            if "started_at" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE ai_vocabulary_runs ADD COLUMN started_at TIMESTAMPTZ")
                )
            if "last_heartbeat_at" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE ai_vocabulary_runs ADD COLUMN last_heartbeat_at TIMESTAMPTZ")
                )
            if "last_progress_message" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE ai_vocabulary_runs ADD COLUMN last_progress_message TEXT")
                )
            conn.execute(
                text(
                    "UPDATE ai_vocabulary_runs "
                    "SET run_key = substring(md5("
                    "coalesce(dataset, '') || '|' || "
                    "coalesce(sample_version_id, '') || '|' || "
                    "coalesce(prompt_version, '') || '|' || "
                    "coalesce(provider, '') || '|' || "
                    "coalesce(model_name, '') || '|' || "
                    "coalesce(temperature::text, '')"
                    "), 1, 32) "
                    "WHERE run_key IS NULL"
                )
            )
            conn.execute(
                text(
                    "WITH ranked AS ("
                    "  SELECT id, run_key, "
                    "         ROW_NUMBER() OVER (PARTITION BY run_key ORDER BY created_at ASC, id ASC) AS rn "
                    "  FROM ai_vocabulary_runs "
                    "  WHERE run_key IS NOT NULL"
                    ") "
                    "UPDATE ai_vocabulary_runs r "
                    "SET run_key = r.run_key || '_' || substring(md5(r.id), 1, 8) "
                    "FROM ranked "
                    "WHERE r.id = ranked.id AND ranked.rn > 1"
                )
            )
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_ai_vocabulary_runs_run_key "
                    "ON ai_vocabulary_runs (run_key) "
                    "WHERE run_key IS NOT NULL"
                )
            )

    if "dataset_sample_versions" in existing_tables:
        existing_columns = {column["name"] for column in inspector.get_columns("dataset_sample_versions")}
        with engine.begin() as conn:
            if "max_chunks_per_doc" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE dataset_sample_versions ADD COLUMN max_chunks_per_doc INTEGER DEFAULT 2")
                )
            if "avg_similarity" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE dataset_sample_versions ADD COLUMN avg_similarity DOUBLE PRECISION")
                )
            if "min_similarity" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE dataset_sample_versions ADD COLUMN min_similarity DOUBLE PRECISION")
                )
            if "cluster_count_estimate" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE dataset_sample_versions ADD COLUMN cluster_count_estimate INTEGER")
                )

    if "ai_vocabulary_terms_raw" in existing_tables:
        existing_columns = {column["name"] for column in inspector.get_columns("ai_vocabulary_terms_raw")}
        with engine.begin() as conn:
            if "evidence_start" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE ai_vocabulary_terms_raw ADD COLUMN evidence_start INTEGER")
                )
            if "evidence_end" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE ai_vocabulary_terms_raw ADD COLUMN evidence_end INTEGER")
                )
            if "ignored_at" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE ai_vocabulary_terms_raw ADD COLUMN ignored_at TIMESTAMPTZ")
                )
            if "ignore_reason" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE ai_vocabulary_terms_raw ADD COLUMN ignore_reason VARCHAR(64)")
                )
            if "ignore_note" not in existing_columns:
                conn.execute(
                    text("ALTER TABLE ai_vocabulary_terms_raw ADD COLUMN ignore_note TEXT")
                )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_ai_vocabulary_terms_raw_run_validation "
                    "ON ai_vocabulary_terms_raw (ai_run_id, validation_status)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_ai_vocabulary_terms_raw_run_confidence "
                    "ON ai_vocabulary_terms_raw (ai_run_id, confidence)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_ai_vocabulary_terms_raw_run_doc "
                    "ON ai_vocabulary_terms_raw (ai_run_id, doc_id)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_ai_vocabulary_terms_raw_ignored_at "
                    "ON ai_vocabulary_terms_raw (ignored_at)"
                )
            )

    if "ai_vocabulary_run_logs" in existing_tables:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_ai_vocabulary_run_logs_run_created_at "
                    "ON ai_vocabulary_run_logs (ai_run_id, created_at)"
                )
            )
