"""Converts native types to TDL types."""

from collections.abc import Collection
from types import GenericAlias
from typing import get_args, get_origin

from spider_py.type.tdl_type import (
    BoolType,
    BytesType,
    ClassType,
    DoubleType,
    FloatType,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    ListType,
    MapType,
    TdlType,
)
from spider_py.type.type import Double, Float, Int8, Int16, Int32, Int64
from spider_py.type.utils import get_class_name

TypeDict = {
    Int8: Int8Type(),
    Int16: Int16Type(),
    Int32: Int32Type(),
    Int64: Int64Type(),
    Float: FloatType(),
    Double: DoubleType(),
    bool: BoolType(),
    bytes: BytesType(),
}


def _to_primitive_tdl_type(native_type: type | GenericAlias) -> TdlType | None:
    """
    Converts a native type to primitive TDL type.
    :param native_type:
    :return:
        - The converted TDL primitive if `native_type` is supported.
        - None if `native_type` is not a primitive Python type.
    :raises TypeError: If `native_type` is a primitive Python type not supported by TDL.
    """
    if isinstance(native_type, type) and native_type in TypeDict:
        return TypeDict[native_type]

    if native_type in (int, float, str, complex):
        msg = f"{native_type} is not a TDL type. Please use the corresponding TDL primitive type."
        raise TypeError(msg)

    return None


def to_tdl_type(native_type: type | GenericAlias) -> TdlType:
    """
    Converts a Python type to TDL type.
    :param native_type:
    :return: The converted TDL type.
    :raises TypeError: If `native_type` is not a valid TDL type.
    """
    primitive_tdl_type = _to_primitive_tdl_type(native_type)
    if primitive_tdl_type is not None:
        return primitive_tdl_type

    if isinstance(native_type, GenericAlias):
        origin = get_origin(native_type)
        if origin is list:
            args = get_args(native_type)
            if len(args) == 0:
                msg = "List does not have an element type."
                raise TypeError(msg)
            arg = args[0]
            return ListType(to_tdl_type(arg))

        if origin is dict:
            args = get_args(native_type)
            msg = "Dict does not have a key/value type."
            if len(args) != 2:  # noqa: PLR2004
                raise TypeError(msg)
            key = args[0]
            value = args[1]
            return MapType(to_tdl_type(key), to_tdl_type(value))

        msg = f"{native_type} is not a valid TDL type."
        raise TypeError(msg)

    if issubclass(native_type, Collection):
        msg = f"{native_type} is not a valid TDL type."
        raise TypeError(msg)

    return ClassType(get_class_name(native_type))


def to_tdl_type_str(native_type: type | GenericAlias) -> str:
    """
    :param native_type: A Python native type.
    :return: A string representation of the TDL type.
    :raises TypeError: If `native_type` is not a valid TDL type.
    """
    return to_tdl_type(native_type).type_str()
