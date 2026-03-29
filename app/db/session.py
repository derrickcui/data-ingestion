from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.utility.config import Config


def _build_database_url() -> str:
    if Config.DATABASE_URL:
        return Config.DATABASE_URL

    if Config.PG_HOST and Config.PG_DB and Config.PG_USER:
        password = Config.PG_PASSWORD or ""
        return f"postgresql+psycopg://{Config.PG_USER}:{password}@{Config.PG_HOST}:{Config.PG_PORT}/{Config.PG_DB}"

    return "sqlite:///./data_ingestion.db"


def _create_engine():
    database_url = _build_database_url()
    connect_args = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False

    return create_engine(
        database_url,
        echo=bool(Config.DATABASE_ECHO),
        future=True,
        connect_args=connect_args,
    )


engine = _create_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def get_db_session() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
