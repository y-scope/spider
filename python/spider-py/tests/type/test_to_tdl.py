"""Unit tests for converting to TDL."""

import pytest

from spider_py import Double, Float, Int8, Int16, Int32, Int64
from spider_py.type import to_tdl_type_str


class TestToTDL:
    """Unit tests for converting to TDL."""

    def test_to_tdl_primitive(self) -> None:
        """Test converting primitive types to TDL Types."""
        assert to_tdl_type_str(bool) == "bool"
        assert to_tdl_type_str(Double) == "double"
        assert to_tdl_type_str(Float) == "float"
        assert to_tdl_type_str(Int8) == "int8"
        assert to_tdl_type_str(Int16) == "int16"
        assert to_tdl_type_str(Int32) == "int32"
        assert to_tdl_type_str(Int64) == "int64"

    def test_to_tdl_list(self) -> None:
        """Test converting lists to TDL Types."""
        assert to_tdl_type_str(list[Int8]) == "List<int8>"
        assert to_tdl_type_str(list[list[Int8]]) == "List<List<int8>>"

    def test_to_tdl_map(self) -> None:
        """Test converting maps to TDL Types."""
        assert to_tdl_type_str(dict[Int8, dict[Int16, Float]]) == "Map<int8,Map<int16,float>>"
        assert to_tdl_type_str(dict[list[Int8], Double]) == "Map<List<int8>,double>"

    def test_to_tdl_class(self) -> None:
        """Test converting class to TDL Types."""
        assert to_tdl_type_str(TestToTDL) == "test_to_tdl.TestToTDL"

    def test_to_tdl_primitive_exception(self) -> None:
        """Test converting unsupported primitive types to TDL Types."""
        with pytest.raises(TypeError):
            to_tdl_type_str(int)
        with pytest.raises(TypeError):
            to_tdl_type_str(float)
        with pytest.raises(TypeError):
            to_tdl_type_str(str)
        with pytest.raises(TypeError):
            to_tdl_type_str(bytes)
        with pytest.raises(TypeError):
            to_tdl_type_str(list)
        with pytest.raises(TypeError):
            to_tdl_type_str(dict)
        with pytest.raises(TypeError):
            to_tdl_type_str(tuple)

    def test_to_tdl_list_exception(self) -> None:
        """Test converting unsupported lists to TDL Types."""
        with pytest.raises(TypeError):
            to_tdl_type_str(list[int])

    def test_to_tdl_map_exception(self) -> None:
        """Test converting unsupported maps to TDL Types."""
        with pytest.raises(TypeError):
            to_tdl_type_str(dict[Int8, int])
        with pytest.raises(TypeError):
            to_tdl_type_str(dict[int, Int8])
        with pytest.raises(TypeError):
            to_tdl_type_str(dict[list[Int16], Int8])
        with pytest.raises(TypeError):
            to_tdl_type_str(dict[Float, Int8])
        with pytest.raises(TypeError):
            to_tdl_type_str(dict[dict[Int8, Float], Int8])
