from fastapi import APIRouter
from app.api.routes.email_ingest import router as email_ingest
from app.api.routes.ingest import router as ingest

router = APIRouter()
router.include_router(ingest, prefix="/ingest", tags=["资源上传导入"])
router.include_router(email_ingest, prefix="/email", tags=["email导入"])

@router.get("/")
def hello_world():
    return {"message": "Hello, World!"}