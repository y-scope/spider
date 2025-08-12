"""Storage backend interface module."""

from abc import ABC, abstractmethod
from collections.abc import Sequence

from spider import core


class StorageError(Exception):
    """Storage error."""

    def __init__(self, message: str) -> None:
        """Initializes storage error."""
        super().__init__(message)


class Storage(ABC):
    """Storage backend interface."""

    @abstractmethod
    def submit_jobs(self, task_graphs: Sequence[core.TaskGraph]) -> None:
        """
        Submit jobs to the storage.
        :param task_graphs: Task graphs to submit.
        :raises StorageError: If the storage operations fail.
        """
