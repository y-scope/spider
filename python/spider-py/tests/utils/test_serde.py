"""Tests for serialization and deserialization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import msgpack

import spider_py
from spider_py.utils.serde import from_serializable, to_serializable

if TYPE_CHECKING:
    from types import GenericAlias


def compare_serde(obj: object, cls: type | GenericAlias) -> None:
    """
    Serializes and then deserializes an object, and checks if the result matches the original
    object.
    :param obj: The object to serialize and deserialize.
    :param cls: The expected type of `obj`.
    """
    serialized = msgpack.packb(to_serializable(obj, cls))
    unpacked_data = msgpack.unpackb(serialized, raw=False, strict_map_key=False)
    deserialized = from_serializable(cls, unpacked_data)
    assert obj == deserialized


@dataclass
class Address:
    """A simple address class for testing"""

    city: str
    zipcode: str


@dataclass
class User:
    """A simple user class for testing"""

    id: spider_py.Int8
    name: list[spider_py.Int8]
    address: Address


class TestMsgpackSerde:
    """Test class for msgpack serialization and deserialization."""

    def test_primitives(self) -> None:
        """Tests serialization and deserialization of primitive types."""
        compare_serde(True, bool)
        compare_serde(spider_py.Int8(1), spider_py.Int8)
        compare_serde(spider_py.Float(0.0), spider_py.Float)
        compare_serde(b"bytes", bytes)
        compare_serde([b"a", b"b"], list[bytes])
        compare_serde(["你好".encode(), "世界".encode(), "🤣".encode()], list[bytes])
        compare_serde([b"\xF0\x28\x8C\xBC", b"\xF0\x80\x80\x80", b"\xF5\x90\x80\x80"], list[bytes])
        compare_serde([spider_py.Int8(1), spider_py.Int8(2)], list[spider_py.Int8])
        compare_serde({spider_py.Int8(1): spider_py.Int8(3)}, dict[spider_py.Int8, spider_py.Int8])
        compare_serde(
            [[spider_py.Int8(1), spider_py.Int8(2)], [spider_py.Int8(3), spider_py.Int8(4)]],
            list[list[spider_py.Int8]],
        )

    def test_class(self) -> None:
        """Tests serialization and deserialization of a custom class."""
        user = User(
            id=spider_py.Int8(1),
            name=[spider_py.Int8(byte) for byte in b"Alice"],
            address=Address(city="Wonderland", zipcode="12345"),
        )
        compare_serde(user, User)
