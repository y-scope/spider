"""Spider client Data module."""

from uuid import uuid4

from spider import core


class Data:
    """Represents a spider client data."""

    def __init__(self, value: bytes) -> None:
        """Initialize the Data object with the given value."""
        self._impl = core.Data(uuid4(), value)
