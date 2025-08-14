"""Storage backend interface module."""

from abc import ABC, abstractmethod
from collections.abc import Sequence

from spider import core
from spider.core import JobStatus


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
        :return: List of jobs representing the submitted jobs.
        :raises StorageError: If the storage operations fail.
        """

    @abstractmethod
    def get_job_status(self, job: core.Job) -> JobStatus:
        """
        Gets the job status. This function does not set the `status` field in jobs.
        :param job:
        :return:
        :raises StorageError: If the storage operations fail.
        """

    @abstractmethod
    def get_job_results(self, job: core.Job) -> object:
        """
        Gets the job's results. This function does not set the `results` field in the job.
        :param job:
        :return:
        :raises StorageError: If the storage operations fail.
        :raises msgpack.exceptions.UnPackError: If deserialization of the result fails.
        """
