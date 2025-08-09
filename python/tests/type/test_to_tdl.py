"""Unit tests for converting to TDL."""

from spider import Double, Float, Int8, Int16, Int32, Int64
from spider.type import to_tdl_type_str


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
