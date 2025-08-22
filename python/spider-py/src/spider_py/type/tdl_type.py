"""Spider TDL types."""

from abc import ABC, abstractmethod

from typing_extensions import override


class TdlType(ABC):
    """Abstract base class for all TDL types."""

    @abstractmethod
    def type_str(self) -> str:
        """:return: String representation of the TDL type."""


class DoubleType(TdlType):
    """TDL double type."""

    @override
    def type_str(self) -> str:
        return "double"


class FloatType(TdlType):
    """TDL float type."""

    @override
    def type_str(self) -> str:
        return "float"


class Int8Type(TdlType):
    """TDL int8 type."""

    @override
    def type_str(self) -> str:
        return "int8"


class Int16Type(TdlType):
    """TDL int16 type."""

    @override
    def type_str(self) -> str:
        return "int16"


class Int32Type(TdlType):
    """TDL int32 type."""

    @override
    def type_str(self) -> str:
        return "int32"


class Int64Type(TdlType):
    """TDL int64 type."""

    @override
    def type_str(self) -> str:
        return "int64"


class BoolType(TdlType):
    """TDL bool type."""

    @override
    def type_str(self) -> str:
        return "bool"


class ClassType(TdlType):
    """TDL Custom class type."""

    def __init__(self, name: str) -> None:
        """
        Creates a TDL custom class type.
        :param name: The name of the class.
        """
        self._name = name

    @override
    def type_str(self) -> str:
        return self._name


class ListType(TdlType):
    """TDL List type."""

    def __init__(self, element_type: TdlType) -> None:
        """Creates a TDL list type."""
        self.element_type = element_type

    @override
    def type_str(self) -> str:
        return f"List<{self.element_type.type_str()}>"


def is_integral(tdl_type: TdlType) -> bool:
    """:return: If TDL type is an integral type."""
    return isinstance(tdl_type, (Int8Type, Int16Type, Int32Type, Int64Type))


def is_string(tdl_type: TdlType) -> bool:
    """:return: If TDL type is a string type, i.e. List<int8>."""
    return isinstance(tdl_type, ListType) and isinstance(tdl_type.element_type, Int8Type)


def is_map_key(tdl_type: TdlType) -> bool:
    """:return: If TDL type is a valid type for map key."""
    return is_integral(tdl_type) or is_string(tdl_type)


class MapType(TdlType):
    """TDL Map type."""

    def __init__(self, key_type: TdlType, value_type: TdlType) -> None:
        """
        Creates a TDL map type.
        :raises TypeError: If key is not a supported type.
        """
        if not is_map_key(key_type):
            msg = f"{key_type} is not a supported type for map key."
            raise TypeError(msg)
        self.key_type = key_type
        self.value_type = value_type

    @override
    def type_str(self) -> str:
        return f"Map<{self.key_type.type_str()},{self.value_type.type_str()}>"
