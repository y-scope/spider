"""Storage backend interface module."""

from abc import ABC, abstractmethod
from collections.abc import Sequence

from spider_py import core
from spider_py.core import JobStatus


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

    @abstractmethod
    def get_job_status(self, job: core.Job) -> JobStatus:
        """
        Gets the job status. This function does not set the `status` field in `job`.
        :param job:
        :return: The job status.
        :raises StorageError: If the storage operations fail.
        """

    @abstractmethod
    def get_job_results(self, job: core.Job) -> list[core.TaskOutput] | None:
        """
        Gets the job results. This function does not set the `results` field in `job`.
        :param job:
        :return: A list of task outputs or None if the job has no results yet.
        :raises StorageError: If the storage operations fail.
        """
