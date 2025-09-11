"""Spider job module."""

import msgpack

from spider_py import core
from spider_py.client.data import Data
from spider_py.storage import Storage, StorageError
from spider_py.type import parse_tdl_type
from spider_py.utils import from_serializable


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
        """:return: The current job status."""
        if self._impl.status != core.JobStatus.Running:
            return self._impl.status
        status = self._storage.get_job_status(self._impl)
        self._impl.status = status
        return status

    def get_results(self) -> object | None:
        """
        :return: The job results if the job ended successfully.
        :return: None if the job is still running or ended unsuccessfully.
        """
        if self._impl.results is None:
            self._impl.results = self._storage.get_job_results(self._impl)

        if self._impl.results is None:
            return None

        return _deserialize_outputs(self._impl.results)


def _deserialize_outputs(outputs: list[core.TaskOutput]) -> tuple[object, ...] | object:
    """
    Deserializes a list of `core.TaskOutput` objects to their Python values.
    :param outputs:
    :return: A tuple containing the deserialized values of `outputs`, or a single value if
     `outputs` contains only one element.
    :raises msgpack.exceptions.UnpackException: If there was an error deserializing the TaskOutput
     values.
    """
    results = []
    for output in outputs:
        if isinstance(output.value, core.TaskOutputValue):
            cls = parse_tdl_type(output.type).native_type()
            unpacked = msgpack.unpackb(output.value, raw=False, strict_map_key=False)
            results.append(from_serializable(cls, unpacked))
        elif isinstance(output.value, core.Data):
            results.append(Data(output.value))
        else:
            msg = "Fail to get data from storage."
            raise StorageError(msg)
    if len(results) == 1:
        return results[0]
    return tuple(results)
