"""Storage backend interface module."""

from abc import ABC, abstractmethod
from collections.abc import Sequence

from spider_py import core


class StorageError(Exception):
    """Storage error."""

    def __init__(self, message: str) -> None:
        """Initializes storage error."""
        super().__init__(message)


class Storage(ABC):
    """Storage backend interface."""

    @abstractmethod
    def submit_jobs(
        self, driver_id: core.DriverId, task_graphs: Sequence[core.TaskGraph]
    ) -> Sequence[core.Job]:
        """
        Submits jobs to the storage.
        :param driver_id: Driver id.
        :param task_graphs: Task graphs to submit.
        :return: A list of submitted jobs.
        :raises StorageError: If the storage operations fail.
        """
