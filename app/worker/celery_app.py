# app/worker/celery_app.py
from celery import Celery
from app.utility.config import Config

# 如果未配置 REDIS_BROKER，会抛错：所以我们不在这里创建 Celery 实例（见下）
# 通过工厂函数根据 config 创建 Celery（在有 REDIS 情况下）
def make_celery():
    if not Config.REDIS_BROKER_URL:
        return None
    celery = Celery(
        "data_ingestion",
        broker=Config.REDIS_BROKER_URL,
        backend=Config.REDIS_BACKEND_URL or Config.REDIS_BROKER_URL,
    )
    celery.autodiscover_tasks(['app.worker.tasks'])
    return celery

# 方便导出
celery_app = make_celery()
