import os
import signal
import subprocess
import time
import uuid
from pathlib import Path

import msgpack
import pytest

from .client import (
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


def start_scheduler_worker_no_exit(storage_url: str, scheduler_port: int):
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
        "--no-exit",
    ]
    worker_process = subprocess.Popen(worker_cmds, start_new_session=True)

    return scheduler_process, worker_process


scheduler_port = 6103


@pytest.fixture(scope="function")
def scheduler_worker_no_exit(storage):
    scheduler_process, worker_process = start_scheduler_worker_no_exit(
        storage_url=storage_url, scheduler_port=scheduler_port
    )
    # Wait for 5 second to make sure the scheduler and worker are started
    time.sleep(5)
    yield scheduler_process, worker_process
    scheduler_process.kill()
    worker_process.kill()


class TestWorkerNoExit:
    def test_no_exit(self, storage, scheduler_worker_no_exit):
        _, worker_process = scheduler_worker_no_exit
        # Send SIGTERM should not kill worker
        os.kill(worker_process.pid, signal.SIGTERM)
        time.sleep(1)
        assert worker_process.poll() is None

        # New task should not be executed
        task = Task(
            id=uuid.uuid4(),
            function_name="sum_test",
            inputs=[
                TaskInput(type="int", value=msgpack.packb(1)),
                TaskInput(type="int", value=msgpack.packb(2)),
            ],
            outputs=[TaskOutput(type="int")],
        )
        graph = TaskGraph(
            id=uuid.uuid4(),
            tasks={task.id: task},
            dependencies=[],
        )
        job_id = uuid.uuid4()
        submit_job(storage, job_id, graph)
        time.sleep(1)
        assert get_task_state(storage, task.id) == "ready"

        # Send a second SIGTERM should not kill worker
        os.kill(worker_process.pid, signal.SIGTERM)
        time.sleep(1)
        assert worker_process.poll() is None

        # Cleanup job
        remove_job(storage, job_id)
