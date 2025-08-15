"""Spider job module."""

from spider import core
from spider.storage import Storage


class Job:
    """Represents Spider job."""

    def __init__(self, job: core.Job, storage: Storage) -> None:
        """
        Creates a new Spider job.
        :param job: Core job object.
        :param storage: The storage backend.
        """
        self._impl = job
        self._storage = storage

    def get_status(self) -> core.JobStatus:
        """
        :return: The current job status.
        :raises StorageError: If there was an error retrieving the job status from storage.
        """
        if self._impl.status != core.JobStatus.Running:
            return self._impl.status

        status = self._storage.get_job_status(self._impl)
        self._impl.status = status
        return status

    def get_results(self) -> object | None:
        """
        :return: The job results or None if the status is not Running.
        :raises StorageError: If there was an error retrieving the job results from storage.
        :raises msgpack.exceptions.UnpackException: If there was an error deserializing the job
         results.
        """
        if self._impl.results is not None:
            return self._impl.results

        results = self._storage.get_job_results(self._impl)
        self._impl.results = results
        return results
