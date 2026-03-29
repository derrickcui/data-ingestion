import logging
import os
import sys


def get_logger():
    log_dir = "/app/logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "llm-rag.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, mode="a", encoding="utf-8"),
        ],
    )

    app_logger = logging.getLogger("app")

    for handler in app_logger.handlers:
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        stream = getattr(handler, "stream", None)
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")

    return app_logger


logger = get_logger()
