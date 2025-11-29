"""
自动加载 app.pipelines 下的所有 Processor 类（继承 BaseProcessor），并实例化它们。
返回实例列表，按 order 排序。
"""

import pkgutil
import importlib
import inspect
from typing import List
from app.utility.log import logger

from app.pipelines.base import BaseProcessor


def load_all_processors() -> List[BaseProcessor]:
    processors = []

    # 导入 app.pipelines 包（必须在 sys.path 中）
    pkg = importlib.import_module("app.pipelines")

    for finder, module_name, ispkg in pkgutil.iter_modules(pkg.__path__):
        # 跳过特殊文件（如 __pycache__）和自身注册器
        if module_name in ("base", "processor_registry"):
            continue

        full_mod_name = f"{pkg.__name__}.{module_name}"
        try:
            module = importlib.import_module(full_mod_name)
        except Exception as e:
            # 记录模块导入失败的原因，以帮助调试
            logger.error(f"Failed to load processor module {full_mod_name}: {e}")
            continue
            # raise RuntimeError(f"Cannot load processor module {full_mod_name}") from e

        # 找到 module 中继承自 BaseProcessor 的类（排除 BaseProcessor 本身）
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, BaseProcessor) and obj is not BaseProcessor:
                try:
                    inst = obj()
                    processors.append(inst)
                except Exception as e:
                    # 如果构造器需要参数，或者构造时抛出异常（例如缺少API Key），则记录警告
                    logger.warning(f"Failed to instantiate processor {obj.__name__} in module {full_mod_name}: {e}")
                    #raise RuntimeError(f"Cannot instantiate processor {obj.__name__}") from e
                    continue

    # 根据 order 排序
    processors.sort(key=lambda p: getattr(p, "order", 100))

    # 构建日志信息
    processor_names = [p.__class__.__name__ for p in processors]
    logger.info(
        f"Successfully loaded {len(processors)} pipeline processors: "
        f"[{', '.join(processor_names)}]. Order: {[(p.__class__.__name__, p.order) for p in processors]}"
    )

    return processors