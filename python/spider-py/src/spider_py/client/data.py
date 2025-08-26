"""Spider client Data module."""

from spider_py import core


class Data:
    """Represents a spider client data."""

    def __init__(self, value: bytes) -> None:
        """Initialize the Data object with the given value."""
        self.data_id = core.DataId()
        self.value = value
