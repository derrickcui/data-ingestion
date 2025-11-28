import logging
import os
import sys

def get_logger():
    log_dir = "/app/logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, 'llm-rag.log')

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),  # 强制日志输出到 stdout
            logging.FileHandler(log_file, mode='a', encoding='utf-8'),
        ]
    )

    app_logger = logging.getLogger("app")

    # 解决 Windows 中文日志问题
    for handler in app_logger.handlers:
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        handler.stream.reconfigure(encoding="utf-8")  # 关键：强制 UTF-8 编码

    return app_logger


logger = get_logger()

# 测试日志输出
logger.debug("This is a debug log.")
logger.info("This is an info log.")
logger.warning("This is a warning log.")
logger.error("This is an error log.")
logger.critical("This is a critical log.")