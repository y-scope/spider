"""Tests for msgpack serialization and deserialization."""

from dataclasses import dataclass

import msgpack

import spider_py
from spider_py.utils.msgpack_serde import msgpack_decoder, msgpack_encoder


def compare_serde(obj: object) -> None:
    """
    Serializes and then deserializes an object, and checks if the result matches the original
    object.
    """
    serialized = msgpack.packb(obj, default=msgpack_encoder)
    unpacked_data = msgpack.unpackb(serialized, raw=False, strict_map_key=False)
    deserialized = msgpack_decoder(type(obj), unpacked_data)
    assert obj == deserialized


@dataclass
class Address:
    """A simple address class for testing"""

    city: str
    zipcode: str


@dataclass
class User:
    """A simple user class for testing"""

    id: int
    name: str
    address: Address


class TestMsgpackSerde:
    """Test class for msgpack serialization and deserialization."""

    def test_primitives(self) -> None:
        """Tests serialization and deserialization of primitive types."""
        compare_serde(True)
        compare_serde([1, 2, 3])
        compare_serde({"key": "value"})
        compare_serde(spider_py.Int8(1))
        compare_serde(spider_py.Float(0.0))
        compare_serde([spider_py.Int8(1), spider_py.Int8(2)])
        compare_serde({spider_py.Int8(1): spider_py.Int8(3)})
        compare_serde(
            [[spider_py.Int8(1), spider_py.Int8(2)], [spider_py.Int8(3), spider_py.Int8(4)]]
        )

    def test_class(self) -> None:
        """Tests serialization and deserialization of a custom class."""
        user = User(id=1, name="Alice", address=Address(city="Wonderland", zipcode="12345"))
        compare_serde(user)
