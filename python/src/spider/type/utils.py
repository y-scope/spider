"""Utility for TDL types."""

from importlib import import_module
from typing import cast


def get_class_name(cls: type) -> str:
    """
    :param cls:
    :return: Full name of `cls`.
    """
    return f"{cls.__module__}.{cls.__qualname__}"


def get_class_by_name(name: str) -> type:
    """
    Gets class by name.
    :param name:
    :return:
    :raise: TypeError if `class_name` is not a valid class.
    """
    parts = name.split(".")
    module_name = ".".join(parts[:-1])
    class_name = parts[-1]
    try:
        module = import_module(module_name)
        return cast("type", getattr(module, class_name))
    except (ValueError, ModuleNotFoundError, AttributeError) as exc:
        msg = f"{name} is not a valid TDL type."
        raise TypeError(msg) from exc
