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
    :return: The type object identified by `name`.
    :raise: TypeError if `class_name` is not a valid class.
    """
    module_name, _, class_name = name.rpartition(".")
    if module_name == "":
        msg = f"{name} does not contain a valid Python module."
        raise TypeError(msg)
    try:
        module = import_module(module_name)
        return cast("type", getattr(module, class_name))
    except (ValueError, ModuleNotFoundError, AttributeError) as e:
        msg = f"{name} is not a valid TDL type."
        raise TypeError(msg) from e
