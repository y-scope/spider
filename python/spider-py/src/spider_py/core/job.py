"""Job module for Spider."""

from enum import IntEnum
from uuid import UUID

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
        The job's status and results are cached from the data in storage. Once the job completes,
        the status and results will remain unchanged.
        :param job_id:
        """
        self.job_id = job_id
        self.status = JobStatus.Running
        self.results: object | None = None
