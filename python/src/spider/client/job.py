"""Spider job module."""

from spider import core
from spider.storage import Storage


class Job:
    """Represents Spider job."""

    def __init__(self, job_id: core.JobId, storage: Storage) -> None:
        """
        Creates a new Spider job.
        :param job_id:
        :param storage: The storage backend.
        """
        self.job_id = job_id
        self.storage = storage
