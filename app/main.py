import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# 假设这些模块导入路径正确，并且 Config 包含所有配置
from app.db.init_db import init_database
from app.utility.log import logger
from app.utility.config import Config
from app.api.router import router

# ----------------------------------------------------
# 优化 1: 信号处理 - 依赖 Uvicorn/ASGI Server
# ----------------------------------------------------
# 在生产环境中，通常由 ASGI 服务器（如 Uvicorn）负责捕获 SIGINT/SIGTERM
# 并调用 lifespan 的 shutdown 部分，因此手动捕获 signal.signal 是多余的。
# 我们移除手动信号捕获，完全依赖 lifespan。
# ----------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 应用生命周期管理器 (推荐的现代做法)。
    负责在应用启动前后的资源初始化和清理。
    """

    # --- 启动事件 (Startup) ---
    logger.info("========================================")
    logger.info(f"🚀 Starting {Config.APP_NAME} (v{Config.VERSION})")
    logger.info(f"✅ Debug mode: {Config.DEBUG}")
    logger.info(f"✅ Log Level: {logging.getLevelName(logger.level)}")
    logger.info("========================================")

    # 可以在这里初始化数据库连接池、Celery Worker 状态等。
    init_database()

    yield  # <-- 应用运行阶段

    # --- 关闭事件 (Shutdown) ---
    logger.info(f"👋 Shutting down {Config.APP_NAME}...")
    # 在这里执行清理操作，例如：
    # - 关闭数据库连接池
    # - 停止后台线程或任务
    logger.info("Cleanup complete.")


# ----------------------------------------------------
# 优化 2: 保持配置和实例创建的简洁性
# ----------------------------------------------------
# 移除冗余的 app2 命名，直接使用 app
app = FastAPI(
    title=Config.APP_NAME,
    version=Config.VERSION,
    debug=Config.DEBUG,
    lifespan=lifespan,  # 正确传入 lifespan
    # 可以在这里添加 openapi_url=None 来禁用 OpenAPI 文档，如果不需要的话
    # openapi_url="/openapi.json" if Config.DEBUG else None
)

# ----------------------------------------------------
# 优化 3: 中间件的清晰配置
# ----------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    # 使用 * 号来提高可读性，但如果 Config.ALLOWED_ORIGINS 包含具体的 URL 列表，则直接使用列表
    allow_origins=Config.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    # 暴露自定义头部应该谨慎，只有在客户端需要读取时才暴露
    expose_headers=["X-Request-ID"],
    max_age=600
)

# 路由包含
app.include_router(router)


# ----------------------------------------------------
# 优化 4: 默认路由的优化
# ----------------------------------------------------
@app.get("/", summary="Root Health Check")
async def root():
    """提供应用的基本信息和健康状态。"""
    return {
        "app_name": Config.APP_NAME,
        "version": Config.VERSION,
        "status": "online"
    }
