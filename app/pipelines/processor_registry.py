# app/pipelines/processor_registry.py
import pkgutil
import importlib
import inspect
from typing import List, Type
from app.utility.log import logger
from app.pipelines.base import BaseProcessor


def load_all_processor_classes() -> List[Type[BaseProcessor]]:
    processor_classes: List[Type[BaseProcessor]] = []
    pkg = importlib.import_module("app.pipelines")

    logger.info("开始自动加载 app.pipelines 下的所有 Processor...")

    for finder, module_name, ispkg in pkgutil.iter_modules(pkg.__path__):
        if module_name in ("base", "processor_registry"):
            continue

        full_mod_name = f"{pkg.__name__}.{module_name}"
        try:
            module = importlib.import_module(full_mod_name)
        except Exception as e:
            logger.error(f"Failed to import {full_mod_name}: {e}")
            continue

        for _, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, BaseProcessor) and obj is not BaseProcessor:
                processor_classes.append(obj)

    processor_classes.sort(key=lambda cls: getattr(cls, "order", 100))
    
    names = [cls.__name__ for cls in processor_classes]
    orders = [(cls.__name__, getattr(cls, "order", 100)) for cls in processor_classes]
    
    logger.info(f"Processor 加载完成！共发现 {len(processor_classes)} 个处理器")
    logger.info(f"处理器列表: {names}")
    logger.info(f"执行顺序 (order): {orders}")

    return processor_classes


# 你目前没用到这个，但留着备用
def load_all_processors() -> List[BaseProcessor]:
    classes = load_all_processor_classes()
    instances = []
    for cls in classes:
        try:
            inst = cls()
            instances.append(inst)
        except Exception as e:
            logger.warning(f"实例化失败 {cls.__name__}: {e}")
    return instances