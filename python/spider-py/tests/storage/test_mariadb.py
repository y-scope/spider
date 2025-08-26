"""Tests for the MariaDB storage backend."""

from uuid import uuid4

import msgpack
import pytest

from spider_py import chain, group, Int8, TaskContext
from spider_py.core import TaskInputValue
from spider_py.storage import MariaDBStorage, parse_jdbc_url

MariaDBTestUrl = "jdbc:mariadb://127.0.0.1:3306/spider-storage?user=spider&password=password"


@pytest.fixture(scope="session")
def mariadb_storage() -> MariaDBStorage:
    """Fixture to create a MariaDB storage instance."""
    params = parse_jdbc_url(MariaDBTestUrl)
    return MariaDBStorage(params)


def double(_: TaskContext, x: Int8) -> Int8:
    """Double a number."""
    return Int8(x * 2)


def swap(_: TaskContext, x: Int8, y: Int8) -> tuple[Int8, Int8]:
    """Swaps two numbers."""
    return y, x


class TestMariaDBStorage:
    """Test class for the MariaDB storage backend."""

    @pytest.mark.storage
    def test_job_submission(self, mariadb_storage: MariaDBStorage) -> None:
        """Test job submission to the MariaDB storage backend."""
        graph = chain(group([double, double, double, double]), group([swap, swap]))._impl
        # Fill input data
        for i, task_id in enumerate(graph.input_tasks):
            task = graph.tasks[task_id]
            task.task_inputs[0].value = TaskInputValue(msgpack.packb(i))

        driver_id = uuid4()
        jobs = mariadb_storage.submit_jobs(driver_id, [graph])
        assert len(jobs) == 1
