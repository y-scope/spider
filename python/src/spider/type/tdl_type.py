"""Spider TDL types."""

from abc import ABC, abstractmethod
from types import GenericAlias

from typing_extensions import override

from spider.type.type import Double, Float, Int8, Int16, Int32, Int64
from spider.type.utils import get_class_by_name


class TdlType(ABC):
    """Abstract base class for all TDL types."""

    @abstractmethod
    def type_str(self) -> str:
        """:return: String representation of the TDL type."""

    @abstractmethod
    def native_type(self) -> type | GenericAlias:
        """:return: Native Python type of the TDL type."""


class DoubleType(TdlType):
    """TDL double type."""

    @override
    def type_str(self) -> str:
        return "double"

    @override
    def native_type(self) -> type | GenericAlias:
        return Double


class FloatType(TdlType):
    """TDL float type."""

    @override
    def type_str(self) -> str:
        return "float"

    @override
    def native_type(self) -> type | GenericAlias:
        return Float


class Int8Type(TdlType):
    """TDL int8 type."""

    @override
    def type_str(self) -> str:
        return "int8"

    @override
    def native_type(self) -> type | GenericAlias:
        return Int8


class Int16Type(TdlType):
    """TDL int16 type."""

    @override
    def type_str(self) -> str:
        return "int16"

    @override
    def native_type(self) -> type | GenericAlias:
        return Int16


class Int32Type(TdlType):
    """TDL int32 type."""

    @override
    def type_str(self) -> str:
        return "int32"

    @override
    def native_type(self) -> type | GenericAlias:
        return Int32


class Int64Type(TdlType):
    """TDL int64 type."""

    @override
    def type_str(self) -> str:
        return "int64"

    @override
    def native_type(self) -> type | GenericAlias:
        return Int64


class BoolType(TdlType):
    """TDL bool type."""

    @override
    def type_str(self) -> str:
        return "bool"

    @override
    def native_type(self) -> type | GenericAlias:
        return bool


class ClassType(TdlType):
    """TDL Custom class type."""

    def __init__(self, name: str) -> None:
        """
        Creates a TDL custom class type.
        :param name: The name of the class.
        """
        self.name = name

    @override
    def type_str(self) -> str:
        return self.name

    @override
    def native_type(self) -> type | GenericAlias:
        """
        :return: Native Python type of the class.
        :raise: TypeError if `class_name` is not a valid class.
        """
        return get_class_by_name(self.name)


class ListType(TdlType):
    """TDL List type."""

    def __init__(self, key: TdlType) -> None:
        """Creates a TDL list type."""
        self.key = key

    @override
    def type_str(self) -> str:
        return f"List<{self.key.type_str()}>"

    @override
    def native_type(self) -> type | GenericAlias:
        return list[self.key.native_type()]  # type: ignore[misc]


def is_integral(tdl_type: TdlType) -> bool:
    """:return: If TDL type is an integral type."""
    return isinstance(tdl_type, (Int8Type, Int16Type, Int32Type, Int64Type))


def is_string(tdl_type: TdlType) -> bool:
    """:return: If TDL type is a string type, i.e. List<int8>."""
    return isinstance(tdl_type, ListType) and isinstance(tdl_type.key, Int8Type)


def is_map_key(tdl_type: TdlType) -> bool:
    """:return: If TDL type is a valid type for map key."""
    return is_integral(tdl_type) or is_string(tdl_type)


class MapType(TdlType):
    """TDL Map type."""

    def __init__(self, key: TdlType, value: TdlType) -> None:
        """
        Creates a TDL map type.
        :raises TypeError: If key is not a supported type.
        """
        if not is_map_key(key):
            msg = f"{key} is not a supported type for map key."
            raise TypeError(msg)
        self.key = key
        self.value = value

    @override
    def type_str(self) -> str:
        return f"Map<{self.key.type_str()},{self.value.type_str()}>"

    @override
    def native_type(self) -> type | GenericAlias:
        return dict[self.key.native_type(), self.value.native_type()]  # type: ignore[misc]
