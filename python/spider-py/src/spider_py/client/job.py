"""Spider job module."""

import msgpack

from spider_py import core
from spider_py.client.data import Data
from spider_py.storage import Storage, StorageError
from spider_py.type import parse_tdl_type
from spider_py.utils import from_serializable


def _convert_outputs(outputs: list[core.TaskOutput]) -> tuple[object, ...]:
    """
    Converts a list of TaskOutput objects to a tuple of their values.
    :param outputs: The list of TaskOutput objects.
    :return: A tuple containing the values of the TaskOutput objects.
    :raises msgpack.exceptions.UnpackException: If there was an error deserializing the TaskOutput
     values.
    :raises StorageError: If there was an error in the TaskOutput values.
    """
    results = []
    for output in outputs:
        if isinstance(output.value, core.TaskOutputValue):
            cls = parse_tdl_type(output.type).native_type()
            unpacked = msgpack.unpackb(output.value, raw=False, strict_map_key=False)
            results.append(from_serializable(cls, unpacked))
        elif isinstance(output.value, core.Data):
            results.append(Data._from_impl(output.value))
        else:
            msg = "Fail to get data from storage."
            raise StorageError(msg)
    return tuple(results)


class Job:
    """Represents Spider job."""

    def __init__(self, job: core.Job, storage: Storage) -> None:
        """
        Creates a new Spider job.
        :param job:
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
            return _convert_outputs(self._impl.results)

        results = self._storage.get_job_results(self._impl)
        if results is None:
            return None
        return _convert_outputs(results)
