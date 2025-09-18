"""Spider client Data module."""

from __future__ import annotations

from uuid import uuid4

from spider_py import core


class Data:
    """Represents a spider client data."""

    def __init__(self, core_data: core.Data) -> None:
        """
        Initializes a data object by the given `core.Data` implementation.

        NOTE: This method is a low-level constructor. Please use `Data.from_value` to create a data
        object from a value.

        :param core_data:
        """
        self._impl = core_data

    @staticmethod
    def from_value(value: bytes) -> Data:
        """
        :param value: The data value.
        :return: A newly created data object with a random UUID and the given value.
        """
        return Data(core.Data(uuid4(), value))

    @property
    def id(self) -> core.DataId:
        """:return: The data id."""
        return self._impl.id

    @property
    def value(self) -> bytes:
        """:return: The data value."""
        return self._impl.value

    @property
    def localities(self) -> list[str]:
        """:return: The data localities."""
        return self._impl.localities

    @localities.setter
    def localities(self, value: list[str]) -> None:
        """
        Sets the data localities.
        :param value: The new localities.
        """
        self._impl.localities = value

    def add_locality(self, addr: str) -> None:
        """
        Adds a new address to the data localities.
        :param addr: The address to add.
        """
        self._impl.localities.append(addr)

    @property
    def hard_locality(self) -> bool:
        """:return: Whether the data has hard locality."""
        return self._impl.hard_locality

    @hard_locality.setter
    def hard_locality(self, hard: bool) -> None:
        """
        Sets whether the data has hard locality.
        :param hard:
        """
        self._impl.hard_locality = hard

    @property
    def persisted(self) -> bool:
        """:return: Whether the data is persisted."""
        return self._impl.persisted

    def set_persisted(self) -> None:
        """Sets the data as persisted."""
        self._impl.persisted = True
