"""Custom type module for Spider."""

from typing import cast


class BoundedInt(int):
    """Bounded integer type."""

    def __new__(cls, value: int, bits: int = 32) -> "BoundedInt":
        """Creates a bounded integer."""
        min_val = -(1 << (bits - 1))
        max_val = (1 << (bits - 1)) - 1

        if not (min_val <= value <= max_val):
            msg = f"Bounded integer value ({value}) must be between {min_val} and {max_val}"
            raise ValueError(msg)

        return super().__new__(cls, value)


class Int8(BoundedInt):
    """8 bits integer type."""

    def __new__(cls, value: int) -> "Int8":
        """Creates an int8 integer."""
        return cast("Int8", super().__new__(cls, value, bits=8))


class Int16(BoundedInt):
    """16 bits integer type."""

    def __new__(cls, value: int) -> "Int16":
        """Creates an int16 integer."""
        return cast("Int16", super().__new__(cls, value, bits=16))


class Int32(BoundedInt):
    """32 bits integer type."""

    def __new__(cls, value: int) -> "Int32":
        """Creates an int32 integer."""
        return cast("Int32", super().__new__(cls, value, bits=8))
