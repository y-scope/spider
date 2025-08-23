"""Custom type module for Spider."""

from __future__ import annotations

from typing import cast


class BoundedInt(int):
    """Bounded integer type."""

    def __new__(cls, value: int, bits: int = 32) -> BoundedInt:
        """Creates a bounded integer."""
        if bits not in (8, 16, 32, 64):
            msg = f"Unsupported bits size: {bits}. Supported sizes are 8, 16, 32, or 64."
            raise ValueError(msg)

        lower_bound = -(1 << (bits - 1))
        upper_bound = (1 << (bits - 1)) - 1

        if not (lower_bound <= value and value <= upper_bound):
            msg = (
                f"Bounded integer value ({value}) must be between {lower_bound} and {upper_bound}."
            )
            raise ValueError(msg)

        return super().__new__(cls, value)


class Int8(BoundedInt):
    """8 bits integer type."""

    def __new__(cls, value: int) -> Int8:
        """Creates an int8 integer."""
        return cast("Int8", super().__new__(cls, value, bits=8))


class Int16(BoundedInt):
    """16 bits integer type."""

    def __new__(cls, value: int) -> Int16:
        """Creates an int16 integer."""
        return cast("Int16", super().__new__(cls, value, bits=16))


class Int32(BoundedInt):
    """32 bits integer type."""

    def __new__(cls, value: int) -> Int32:
        """Creates an int32 integer."""
        return cast("Int32", super().__new__(cls, value, bits=32))


class Int64(BoundedInt):
    """64 bits integer type."""

    def __new__(cls, value: int) -> Int64:
        """Creates an int64 integer."""
        return cast("Int64", super().__new__(cls, value, bits=64))


class Float(float):
    """Float type."""

    def __new__(cls, value: float) -> Float:
        """Creates a float number."""
        return super().__new__(cls, value)


class Double(float):
    """Double type."""

    def __new__(cls, value: float) -> Double:
        """Creates a double number."""
        return super().__new__(cls, value)
