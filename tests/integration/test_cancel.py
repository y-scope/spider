import subprocess
import time
import uuid
from pathlib import Path
from typing import Tuple

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
) -> Tuple[subprocess.Popen, subprocess.Popen]:
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
def scheduler_worker(storage):
    scheduler_process, worker_process = start_scheduler_worker(
        storage_url=g_storage_url, scheduler_port=g_scheduler_port
    )
    # Wait for 5 second to make sure the scheduler and worker are started
    time.sleep(5)
    yield
    scheduler_process.kill()
    worker_process.kill()


class TestCancel:

    # Test that the task can be cancelled by user and from the task.
    # Execute the cancel_test client, which includes cancelling a running task
    # and executing a task that cancels itself.
    def test_task_cancel(self, scheduler_worker):
        dir_path = Path(__file__).resolve().parent
        dir_path = dir_path / ".."
        client_cmds = [
            str(dir_path / "cancel_test"),
            "--storage_url",
            g_storage_url,
        ]
        p = subprocess.run(client_cmds, timeout=20)
        assert p.returncode == 0
