"""Integration tests for the scheduler and worker processes."""

import subprocess
import time
import uuid
from collections.abc import Generator
from pathlib import Path

import msgpack
import pytest

from .client import (
    add_driver,
    add_driver_data,
    Data,
    Driver,
    g_storage_url,
    get_task_outputs,
    get_task_state,
    remove_data,
    remove_job,
    storage,
    submit_job,
    Task,
    TaskGraph,
    TaskInput,
    TaskOutput,
)
from .utils import g_scheduler_port


def start_scheduler_worker(
    storage_url: str, scheduler_port: int
) -> tuple[subprocess.Popen, subprocess.Popen]:
    """
    Start a scheduler and a worker process.
    :param storage_url: JDBC storage URL
    :param scheduler_port: the port for the scheduler to listen on
    :return: scheduler_process, worker_process
    """
    # Start the scheduler
    dir_path = Path(__file__).resolve().parent
    dir_path = dir_path / ".." / ".." / "src" / "spider"
    scheduler_cmds = [
        str(dir_path / "spider_scheduler"),
        "--host",
        "127.0.0.1",
        "--port",
        str(scheduler_port),
        "--storage_url",
        storage_url,
    ]
    scheduler_process = subprocess.Popen(scheduler_cmds)
    worker_cmds = [
        str(dir_path / "spider_worker"),
        "--host",
        "127.0.0.1",
        "--storage_url",
        storage_url,
        "--libs",
        "tests/libworker_test.so",
    ]
    worker_process = subprocess.Popen(worker_cmds)
    return scheduler_process, worker_process


@pytest.fixture(scope="class")
def scheduler_worker(storage) -> Generator[None, None, None]:
    """
    Fixture to start a scheduler and a worker process. Yields control to the test function.
    After the test function completes, it kills the scheduler and the worker process.
    :return:
    """
    scheduler_process, worker_process = start_scheduler_worker(
        storage_url=g_storage_url, scheduler_port=g_scheduler_port
    )
    # Wait for 5 second to make sure the scheduler and worker are started
    time.sleep(5)
    yield
    scheduler_process.kill()
    worker_process.kill()


@pytest.fixture
def success_job(storage) -> Generator[tuple[TaskGraph, Task, Task, Task], None, None]:
    """
    Fixture to create a job with two parent tasks and one child task. Yields the task graph and
    tasks. Cleans up the job after the test function completes.
    :param storage:
    :return:
    """
    parent_1 = Task(
        id=uuid.uuid4(),
        function_name="sum_test",
        inputs=[
            TaskInput(type="int", value=msgpack.packb(1)),
            TaskInput(type="int", value=msgpack.packb(2)),
        ],
        outputs=[TaskOutput(type="int")],
    )
    parent_2 = Task(
        id=uuid.uuid4(),
        function_name="sum_test",
        inputs=[
            TaskInput(type="int", value=msgpack.packb(3)),
            TaskInput(type="int", value=msgpack.packb(4)),
        ],
        outputs=[TaskOutput(type="int")],
    )
    child = Task(
        id=uuid.uuid4(),
        function_name="sum_test",
        inputs=[
            TaskInput(type="int", task_output=(parent_1.id, 0)),
            TaskInput(type="int", task_output=(parent_2.id, 0)),
        ],
        outputs=[TaskOutput(type="int")],
    )
    graph = TaskGraph(
        tasks={parent_1.id: parent_1, parent_2.id: parent_2, child.id: child},
        dependencies=[(parent_1.id, child.id), (parent_2.id, child.id)],
        id=uuid.uuid4(),
    )

    submit_job(storage, uuid.uuid4(), graph)
    assert (
        get_task_state(storage, parent_1.id) == "ready"
        or get_task_state(storage, parent_1.id) == "running"
        or get_task_state(storage, parent_1.id) == "success"
    )
    assert (
        get_task_state(storage, parent_2.id) == "ready"
        or get_task_state(storage, parent_2.id) == "running"
        or get_task_state(storage, parent_2.id) == "success"
    )
    assert (
        get_task_state(storage, child.id) == "pending"
        or get_task_state(storage, child.id) == "running"
        or get_task_state(storage, child.id) == "success"
    )
    print("success job task ids:", parent_1.id, parent_2.id, child.id)

    yield graph, parent_1, parent_2, child

    remove_job(storage, graph.id)


@pytest.fixture
def fail_job(storage) -> Generator[Task, None, None]:
    """
    Fixture to create a job that will fail. The task will raise an error when executed.
    Yield the task. Cleanup the job after the test function completes.
    :param storage:
    :return:
    """
    task = Task(
        id=uuid.uuid4(),
        function_name="error_test",
        inputs=[TaskInput(type="int", value=msgpack.packb(1))],
        outputs=[TaskOutput(type="int")],
    )
    graph = TaskGraph(
        tasks={task.id: task},
        dependencies=[],
        id=uuid.uuid4(),
    )

    submit_job(storage, uuid.uuid4(), graph)
    print("fail job task id:", task.id)

    yield task

    remove_job(storage, graph.id)


@pytest.fixture
def data_job(storage) -> Generator[Task, None, None]:
    """
    Fixture to create a job that uses data. Yields the task that uses data.
    Cleans up the job and data after the test function completes.
    :param storage:
    :return:
    """
    data = Data(
        id=uuid.uuid4(),
        value=msgpack.packb(2),
    )
    driver = Driver(id=uuid.uuid4())
    add_driver(storage, driver)
    add_driver_data(storage, driver, data)

    task = Task(
        id=uuid.uuid4(),
        function_name="data_test",
        inputs=[TaskInput(type="Data", data_id=data.id)],
        outputs=[TaskOutput(type="int")],
    )
    graph = TaskGraph(
        tasks={task.id: task},
        dependencies=[],
        id=uuid.uuid4(),
    )

    submit_job(storage, uuid.uuid4(), graph)
    print("data job task id:", task.id)

    yield task

    remove_job(storage, graph.id)
    remove_data(storage, data)


@pytest.fixture
def random_fail_job(storage):
    """
    Fixture to create a job that randomly fails. The task will succeed after a few retries.
    Yields the task. Cleans up the job after the test function completes.
    :param storage:
    :return:
    """
    data = Data(
        id=uuid.uuid4(),
        value=msgpack.packb(2),
    )
    driver = Driver(id=uuid.uuid4())
    add_driver(storage, driver)
    add_driver_data(storage, driver, data)

    task = Task(
        id=uuid.uuid4(),
        function_name="random_fail_test",
        inputs=[TaskInput(type="int", value=msgpack.packb(20))],
        outputs=[TaskOutput(type="int")],
        max_retries=5,
    )
    graph = TaskGraph(
        tasks={task.id: task},
        dependencies=[],
        id=uuid.uuid4(),
    )

    submit_job(storage, uuid.uuid4(), graph)
    print("random fail job task id:", task.id)

    yield task

    remove_job(storage, graph.id)
    remove_data(storage, data)


class TestSchedulerWorker:
    """Test class for the scheduler and worker integration tests."""

    @pytest.mark.usefixtures("scheduler_worker")
    def test_job_success(self, storage, success_job):
        """
        Test the successful execution of a job with two parent tasks and one child task.
        :param storage:
        :param success_job:
        :return: None
        """
        graph, parent_1, parent_2, child = success_job
        # Wait for 2 seconds and check task state and output
        time.sleep(2)
        state = get_task_state(storage, parent_1.id)
        assert state == "success"
        outputs = get_task_outputs(storage, parent_1.id)
        assert len(outputs) == 1
        assert outputs[0].value == msgpack.packb(3)
        state = get_task_state(storage, parent_2.id)
        assert state == "success"
        outputs = get_task_outputs(storage, parent_2.id)
        assert len(outputs) == 1
        assert outputs[0].value == msgpack.packb(7)
        state = get_task_state(storage, child.id)
        assert state == "success"
        outputs = get_task_outputs(storage, child.id)
        assert len(outputs) == 1
        assert outputs[0].value == msgpack.packb(10)

    @pytest.mark.usefixtures("scheduler_worker")
    def test_job_failure(self, storage, fail_job):
        """
        Test the failure of a job that raise an error.
        :param storage:
        :param fail_job:
        :return: None
        """
        task = fail_job
        # Wait for 2 seconds and check task output
        time.sleep(2)
        state = get_task_state(storage, task.id)
        assert state == "fail"

    @pytest.mark.usefixtures("scheduler_worker")
    def test_data_job(self, storage, data_job):
        """
        Test the successful execution of a job that uses data.
        :param storage:
        :param data_job:
        :return: None
        """
        task = data_job
        # Wait for 2 seconds and check task output
        time.sleep(2)
        state = get_task_state(storage, task.id)
        assert state == "success"
        outputs = get_task_outputs(storage, task.id)
        assert len(outputs) == 1
        assert outputs[0].value == msgpack.packb(2)

    @pytest.mark.usefixtures("scheduler_worker")
    def test_random_fail_job(self, storage, random_fail_job):
        """
        Test the successful recovery and execution of a job that randomly fails.
        :param storage:
        :param random_fail_job:
        :return: None
        """
        task = random_fail_job
        # Wait for 2 seconds and check task output
        time.sleep(2)
        state = get_task_state(storage, task.id)
        assert state == "success"
