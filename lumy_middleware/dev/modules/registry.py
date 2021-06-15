import logging

from typing import Dict, Callable, Optional

logger = logging.getLogger(__name__)


__dharpa_modules_registry: Dict[str, Callable] = {}


def dharpa_module(module_name):
    def decorator(func):
        if module_name in __dharpa_modules_registry:
            logger.warn(f'Module "{module_name}" is being registered \
                more than once with "{func.__name__}"')
        __dharpa_modules_registry[module_name] = func
        return func
    return decorator


def get_module_processor(module_name) -> Optional[Callable]:
    return __dharpa_modules_registry.get(module_name, None)
