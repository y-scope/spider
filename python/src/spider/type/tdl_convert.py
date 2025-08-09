"""Converts native types to TDL types."""

import types
from collections.abc import Collection
from typing import get_args, get_origin

from spider.type.tdl_type import (
    BoolType,
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
from spider.type.type import Double, Float, Int8, Int16, Int32, Int64


def to_primitive_tdl_type(native_type: type) -> TdlType | None:
    """
    Converts a native type to primitive TDL type.
    :param native_type:
    :return: Converted TDL primitive. None if `native_type` is not a supported primitive type.
    """
    tdl_type: TdlType | None = None
    if native_type is Int8:
        tdl_type = Int8Type()
    elif native_type is Int16:
        tdl_type = Int16Type()
    elif native_type is Int32:
        tdl_type = Int32Type()
    elif native_type is Int64:
        tdl_type = Int64Type()
    elif native_type is Float:
        tdl_type = FloatType()
    elif native_type is Double:
        tdl_type = DoubleType()
    elif native_type is bool:
        tdl_type = BoolType()
    return tdl_type


def to_tdl_type(native_type: type) -> TdlType:
    """
    Converts a Python type to TDL type.
    :param native_type:
    :return:
    :raise: TypeError if `native_type` is not a valid TDL type.
    """
    primitive_tdl_type = to_primitive_tdl_type(native_type)
    if primitive_tdl_type is not None:
        return primitive_tdl_type

    if native_type in (int, float, str, complex, bytes):
        msg = f"{native_type} is not a valid TDL type."
        raise TypeError(msg)

    if isinstance(native_type, types.GenericAlias):
        origin = get_origin(native_type)
        if origin is list:
            arg = get_args(native_type)
            if arg is None:
                msg = "List does not have a key type."
                raise TypeError(msg)
            arg = arg[0]
            return ListType(to_tdl_type(arg))

        if origin is dict:
            arg = get_args(native_type)
            msg = "Dict does not have a key/value type."
            if arg is None:
                raise TypeError(msg)
            if len(arg) != 2:  # noqa: PLR2004
                raise TypeError(msg)
            key = arg[0]
            value = arg[1]
            return MapType(to_tdl_type(key), to_tdl_type(value))

        msg = f"{native_type} is not a valid TDL type."
        raise TypeError(msg)

    if issubclass(native_type, Collection):
        msg = f"{native_type} is not a valid TDL type."
        raise TypeError(msg)

    return ClassType(native_type.__name__)


def to_tdl_type_str(native_type: type) -> str:
    """
    Converts a Python type to TDL type string.
    :param native_type:
    :return:
    :raise: TypeError if `native_type` is not a valid TDL type.
    """
    return to_tdl_type(native_type).type_str()
