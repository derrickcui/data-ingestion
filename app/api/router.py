from fastapi import APIRouter
from app.api.routes.file_ingest import router as file_ingest
from app.api.routes.ingest import router as ingest

router = APIRouter()
router.include_router(ingest, prefix="/ingest", tags=["资源同意上传导入"])
router.include_router(file_ingest, prefix="/file_ingest", tags=["文件上传导入"])

@router.get("/")
def hello_world():
    return {"message": "Hello, World!"}