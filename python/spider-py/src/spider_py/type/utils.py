"""Utility for TDL types."""


def get_class_name(cls: type) -> str:
    """
    :param cls:
    :return: Full name of `cls`.
    """
    return f"{cls.__module__}.{cls.__qualname__}"
