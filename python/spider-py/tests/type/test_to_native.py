"""Test converting TDL type to native type."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import spider_py
from spider_py.type.tdl_parse import parse_tdl_type
from spider_py.type.utils import get_class_name

if TYPE_CHECKING:
    from types import GenericAlias


def _string_to_native(s: str) -> type | GenericAlias:
    """
    Converts a TDL type string to a native type object.
    :param s: The TDL type string to convert.
    :return: The converted native type.
    """
    return parse_tdl_type(s).native_type()


class TestToNativeType:
    """Test converting TDL type to native type."""

    def test_to_primitive_native_type(self) -> None:
        """Test converting primitive TDL type to native type."""
        assert _string_to_native("bool") is bool
        assert _string_to_native("double") is spider_py.Double
        assert _string_to_native("float") is spider_py.Float
        assert _string_to_native("int8") is spider_py.Int8
        assert _string_to_native("int16") is spider_py.Int16
        assert _string_to_native("int32") is spider_py.Int32
        assert _string_to_native("int64") is spider_py.Int64

    def test_to_class_type(self) -> None:
        """Test converting class TDL type to native type."""
        assert _string_to_native(get_class_name(TestToNativeType)) == TestToNativeType
        with pytest.raises(TypeError):
            _string_to_native("NonExistType")
        with pytest.raises(TypeError):
            _string_to_native("NonExistModule.NonExistType")

    def test_to_list_type(self) -> None:
        """Test converting list TDL type to native type."""
        assert _string_to_native("List<int8>") == list[spider_py.Int8]
        assert _string_to_native("List<List<int8>>") == list[list[spider_py.Int8]]

    def test_to_map_type(self) -> None:
        """Test converting map TDL type to native type."""
        assert _string_to_native("Map<int8,int8>") == dict[spider_py.Int8, spider_py.Int8]
        assert (
            _string_to_native("Map<List<int8>,Map<int8,double>>")
            == dict[list[spider_py.Int8], dict[spider_py.Int8, spider_py.Double]]
        )
