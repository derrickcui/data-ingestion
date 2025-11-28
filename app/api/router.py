from fastapi import APIRouter
from app.api.routes.file_ingest import router as file_ingest

router = APIRouter()
router.include_router(file_ingest, prefix="/file_ingest", tags=["User Management"])

@router.get("/")
def hello_world():
    return {"message": "Hello, World!"}