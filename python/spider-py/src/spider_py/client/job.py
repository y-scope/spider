"""Spider job module."""

from __future__ import annotations

import msgpack

from spider_py import core
from spider_py.client.data import Data
from spider_py.storage import Storage, StorageError
from spider_py.storage.job_utils import fetch_and_update_job_results, fetch_and_update_job_status
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
        if self._impl.is_running():
            fetch_and_update_job_status(self._storage, self._impl)
        return self._impl.status

    def get_results(self) -> object | None:
        """
        :return: The job results if the job ended successfully.
        :return: None if the job is still running or ended unsuccessfully.
        """
        fetch_and_update_job_results(self._storage, self._impl)

        if self._impl.results is None:
            return None

        return _deserialize_outputs(self._impl.results)


def _deserialize_outputs(outputs: list[core.TaskOutput]) -> tuple[object, ...] | object:
    """
    Deserializes a list of `core.TaskOutput` objects into their corresponding Python values.
    :param outputs:
    :return: A tuple of deserialized values if `outputs` contains more than one element.
    :return: A single value if `outputs` contains only one element.
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
            msg = "Unsupported output type."
            raise StorageError(msg)
    if len(results) == 1:
        return results[0]
    return tuple(results)
