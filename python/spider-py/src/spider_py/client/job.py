"""Spider job module."""

from spider_py import core
from spider_py.storage import Storage


class Job:
    """Represents Spider job."""

    def __init__(self, job: core.Job, storage: Storage) -> None:
        """
        Creates a new Spider job.
        :param job:
        :param storage: The storage backend.
        """
        self._impl = job
        self.storage = storage
