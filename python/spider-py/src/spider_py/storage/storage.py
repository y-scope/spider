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
        :return: A list of task outputs if all tasks are finished.
        :return: None if any task output is not ready.
        :raises StorageError: If the storage operations fail.
        """

    @abstractmethod
    def create_data_with_driver_ref(self, driver_id: core.DriverId, data: core.Data) -> None:
        """
        Creates a data object in the storage with the given driver references to the data. This
        reference is used for garbage collection purposes.
        :param driver_id: The driver ID to associate with the data.
        :param data:
        :raises StorageError: If the storage operations fail.
        """

    @abstractmethod
    def create_data_with_task_ref(self, task_id: core.TaskId, data: core.Data) -> None:
        """
        Creates a data object in the storage with the given task references to the data. This
        reference is used for garbage collection purposes.
        :param task_id: The task ID to associate with the data.
        :param data:
        :raises StorageError: If the storage operations fail.

        """

    @abstractmethod
    def get_data(self, data_id: core.DataId) -> core.Data:
        """
        Gets the data object associated with the specified data ID from the storage.
        :param data_id:
        :return: The data object associated with `data_id`.
        :raises StorageError: If the storage operations fail.
        """

    @abstractmethod
    def create_driver(self, driver_id: core.DriverId) -> None:
        """
        Creates a driver in the storage.
        :param driver_id:
        :raises StorageError: If the storage operations fail.
        """
