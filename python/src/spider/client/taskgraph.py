"""Spider client TaskGraph module."""

from spider import core


class TaskGraph:
    """
    Spider client TaskGraph class.
    Warps around the core TaskGraph class.
    """

    def __init__(self) -> None:
        """Initialize TaskGraph."""
        self._impl = core.TaskGraph()
