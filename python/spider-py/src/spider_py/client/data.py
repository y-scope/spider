"""Spider client Data module."""

from spider_py import core


class Data:
    """Represents a spider client data."""

    def __init__(self, value: bytes) -> None:
        """
        Initializes a data object by the given value.
        :param value:
        """
        self.data_id = core.DataId()
        self.value = value
