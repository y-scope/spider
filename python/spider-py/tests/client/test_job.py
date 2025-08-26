"""Tests for client job module."""

import pytest
from test_driver import driver  # noqa: F401
from test_task_graph import double, swap

from spider_py import chain, Driver, group, Int8, JobStatus


class TestJob:
    """Tests for client job module."""

    @pytest.mark.storage
    def test_job(self, driver: Driver) -> None:  # noqa: F811
        """Test getting running job status and results."""
        jobs = driver.submit_jobs(
            [
                group([double, double]),
                chain(group([double, double]), swap),
            ],
            [
                (Int8(1), Int8(2)),
                (Int8(1), Int8(2)),
            ],
        )

        assert jobs[0].get_status() == JobStatus.Running
        assert jobs[1].get_status() == JobStatus.Running
        assert jobs[0].get_results() is None
        assert jobs[1].get_results() is None
