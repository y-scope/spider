"""Spider client Data module."""

from uuid import uuid4

from spider_py import core


class Data:
    """Represents a spider client data."""

    def __init__(self, value: bytes) -> None:
        """Initialize the Data object with the given value."""
        self._impl = core.Data(uuid4(), value)

    @staticmethod
    def _from_impl(impl: core.Data) -> "Data":
        """Creates a Data instance from an existing core.Data implementation."""
        data = Data(impl.value)
        data._impl = impl
        return data

    @property
    def value(self) -> bytes:
        """Property to get the value of the data."""
        return self._impl.value

    @property
    def hard_locality(self) -> bool:
        """Property to check if the data has hard locality."""
        return self._impl.hard_locality

    @hard_locality.setter
    def hard_locality(self, value: bool) -> None:
        """Sets the hard locality for the data."""
        self._impl.hard_locality = value

    def get_localities(self) -> list[str]:
        """Gets the list of localities where the data is stored."""
        return [locality.address for locality in self._impl.localities]

    def add_locality(self, address: str) -> None:
        """Adds a new locality to the data."""
        self._impl.localities.append(core.DataLocality(address))
