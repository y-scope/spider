"""Spider job module."""

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

    def get_results(self) -> tuple[object, ...] | None:
        """
        :return: The job results if the job ended successfully.
        :return: None if the job is still running or ended unsuccessfully.
        """
        fetch_and_update_job_results(self._storage, self._impl)

        if self._impl.results is None:
            return None

        return _deserialize_outputs(self._impl.results)


def _deserialize_outputs(outputs: list[core.TaskOutput]) -> tuple[object, ...]:
    """
    Deserializes a list of `core.TaskOutput` objects into their corresponding Python values.
    :param outputs:
    :return: A tuple of deserialized values.
    """
    results = []
    for output in outputs:
        if isinstance(output.value, core.TaskOutputValue):
            type_name = output.type
            if isinstance(type_name, bytes):
                type_name = type_name.decode("utf-8")
            cls = parse_tdl_type(type_name).native_type()
            unpacked = msgpack.unpackb(output.value, raw=False, strict_map_key=False)
            results.append(from_serializable(cls, unpacked))
        elif isinstance(output.value, core.Data):
            results.append(Data(output.value))
        else:
            msg = "Unsupported output type."
            raise StorageError(msg)
    return tuple(results)
