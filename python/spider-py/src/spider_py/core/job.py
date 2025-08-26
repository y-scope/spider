"""Job module for Spider."""

from enum import IntEnum
from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from spider_py.core.task import TaskOutput

JobId = UUID


class JobStatus(IntEnum):
    """Job status."""

    Running = 0
    Succeeded = 1
    Failed = 2
    Cancelled = 3


class Job:
    """Represents a submitted job."""

    def __init__(self, job_id: JobId) -> None:
        """
        Initializes a running job.
        Status and results are cache for the data in storage.
        Once a job completes, the status and results won't change.
        :param job_id:
        """
        self.job_id = job_id
        self.status = JobStatus.Running
        self.results: list[TaskOutput] | None = None
