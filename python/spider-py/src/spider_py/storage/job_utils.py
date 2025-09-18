"""Utility functions for job management."""

from spider_py import core

from .storage import Storage


def fetch_and_update_job_status(storage: Storage, job: core.Job) -> None:
    """
    Fetches and updates the job status from the storage.
    :param storage:
    :param job:
    """
    job.status = storage.get_job_status(job)


def fetch_and_update_job_results(storage: Storage, job: core.Job) -> None:
    """
    Fetches and updates the job status from the storage.

    NOTE: If the job results are already set, this method returns directly.

    :param storage:
    :param job:
    """
    if job.results is not None:
        return
    job.results = storage.get_job_results(job)
