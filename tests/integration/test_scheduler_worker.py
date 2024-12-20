import subprocess
import time
import uuid
from pathlib import Path
from typing import Tuple

import msgpack
import pytest

from .client import (
    get_task_outputs,
    get_task_state,
    remove_job,
    storage,
    storage_url,
    submit_job,
    Task,
    TaskGraph,
    TaskInput,
    TaskOutput,
)


def start_scheduler_worker(
    storage_url: str, scheduler_port: int
) -> Tuple[subprocess.Popen, subprocess.Popen]:
    # Start the scheduler
    dir_path = Path(__file__).resolve().parent
    dir_path = dir_path / ".." / ".." / "src" / "spider"
    scheduler_cmds = [
        str(dir_path / "spider_scheduler"),
        "--port",
        str(scheduler_port),
        "--storage_url",
        storage_url,
    ]
    scheduler_process = subprocess.Popen(scheduler_cmds)
    worker_cmds = [
        str(dir_path / "spider_worker"),
        "--storage_url",
        storage_url,
        "--libs",
        "tests/libworker_test.so",
    ]
    worker_process = subprocess.Popen(worker_cmds)
    return scheduler_process, worker_process


scheduler_port = 6103


@pytest.fixture(scope="class")
def scheduler_worker(storage):
    scheduler_process, worker_process = start_scheduler_worker(
        storage_url=storage_url, scheduler_port=scheduler_port
    )
    yield
    scheduler_process.kill()
    worker_process.kill()


@pytest.fixture(scope="function")
def success_job(storage):
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
    assert get_task_state(storage, parent_1.id) == "ready"
    assert get_task_state(storage, parent_2.id) == "ready"
    assert get_task_state(storage, child.id) == "pending"
    print("success job task ids:", parent_1.id, parent_2.id, child.id)

    yield graph, parent_1, parent_2, child

    remove_job(storage, graph.id)


@pytest.fixture(scope="function")
def fail_job(storage):
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


class TestSchedulerWorker:
    def test_job_success(self, scheduler_worker, storage, success_job):
        graph, parent_1, parent_2, child = success_job
        # Wait for 2 seconds and check task state and output
        time.sleep(2)
        state = get_task_state(storage, parent_1.id)
        assert state == "success"
        outputs = get_task_outputs(storage, parent_1.id)
        assert len(outputs) == 1
        assert outputs[0].value == msgpack.packb(3).decode("utf-8")
        state = get_task_state(storage, parent_2.id)
        assert state == "success"
        outputs = get_task_outputs(storage, parent_2.id)
        assert len(outputs) == 1
        assert outputs[0].value == msgpack.packb(7).decode("utf-8")
        state = get_task_state(storage, child.id)
        assert state == "success"
        outputs = get_task_outputs(storage, child.id)
        assert len(outputs) == 1
        assert outputs[0].value == msgpack.packb(10).decode("utf-8")

    def test_job_failure(self, scheduler_worker, storage, fail_job):
        task = fail_job
        # Wait for 2 seconds and check task output
        time.sleep(2)
        state = get_task_state(storage, task.id)
        assert state == "fail"