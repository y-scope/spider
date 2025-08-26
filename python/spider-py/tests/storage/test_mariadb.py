"""Tests for the MariaDB storage backend."""

from uuid import uuid4

import msgpack
import pytest

from spider_py import chain, group, Int8, TaskContext
from spider_py.core import Data, DataLocality, DriverId, Job, JobStatus, TaskInputValue
from spider_py.storage import MariaDBStorage, parse_jdbc_url, StorageError

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


@pytest.fixture
def submit_job(mariadb_storage: MariaDBStorage) -> Job:
    """Submits a simple job."""
    graph = chain(group([double, double]), group([swap]))._impl
    # Fill input data
    for i, task_id in enumerate(graph.input_tasks):
        task = graph.tasks[task_id]
        task.task_inputs[0].value = TaskInputValue(msgpack.packb(i))

    driver_id = uuid4()
    jobs = mariadb_storage.submit_jobs(driver_id, [graph])
    return jobs[0]


@pytest.fixture
def driver(mariadb_storage: MariaDBStorage) -> DriverId:
    """Fixture to create a driver."""
    driver_id = uuid4()
    mariadb_storage.create_driver(driver_id)
    return driver_id


class TestMariaDBStorage:
    """Test class for the MariaDB storage backend."""

    @pytest.mark.storage
    def test_job_submission(self, mariadb_storage: MariaDBStorage) -> None:
        """Tests job submission to the MariaDB storage backend."""
        graph = chain(group([double, double, double, double]), group([swap, swap]))._impl
        # Fill input data
        for i, task_id in enumerate(graph.input_tasks):
            task = graph.tasks[task_id]
            task.task_inputs[0].value = TaskInputValue(msgpack.packb(i))

        driver_id = uuid4()
        jobs = mariadb_storage.submit_jobs(driver_id, [graph])
        assert len(jobs) == 1

    @pytest.mark.storage
    def test_running_job_status(self, mariadb_storage: MariaDBStorage, submit_job: Job) -> None:
        """Tests getting status of a running job."""
        status = mariadb_storage.get_job_status(submit_job)
        assert status == JobStatus.Running

    @pytest.mark.storage
    def test_running_job_result(self, mariadb_storage: MariaDBStorage, submit_job: Job) -> None:
        """Tests getting results of a running job."""
        results = mariadb_storage.get_job_results(submit_job)
        assert results is None

    @pytest.mark.storage
    def test_data(self, mariadb_storage: MariaDBStorage, driver: DriverId) -> None:
        """Tests data storage and retrieval."""
        value = b"test data"
        data = Data(id=uuid4(), value=value, localities=[DataLocality("localhost")])
        mariadb_storage.create_driver_data(driver, data)
        retrieved_data = mariadb_storage.get_data(data.id)
        assert retrieved_data is not None
        assert retrieved_data.id == data.id
        assert retrieved_data.value == value
        assert retrieved_data.hard_locality == data.hard_locality
        assert retrieved_data.localities == data.localities

    @pytest.mark.storage
    def test_create_data_fail(self, mariadb_storage: MariaDBStorage) -> None:
        """Tests creating data without a driver fails."""
        value = b"test data"
        data = Data(id=uuid4(), value=value, localities=[DataLocality("localhost")])
        with pytest.raises(StorageError):
            mariadb_storage.create_driver_data(uuid4(), data)
