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
        self.storage = storage
