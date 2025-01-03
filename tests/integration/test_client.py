import subprocess
import time
from pathlib import Path
from typing import Tuple

import pytest

from .client import (
    storage,
    storage_url,
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
    # Wait for 5 second to make sure the scheduler and worker are started
    time.sleep(5)
    yield
    scheduler_process.kill()
    worker_process.kill()


class TestClient:
    def test_client(self, scheduler_worker):
        dir_path = Path(__file__).resolve().parent
        dir_path = dir_path / ".."
        client_cmds = [
            str(dir_path / "client_test"),
            "--storage_url",
            storage_url,
        ]
        p = subprocess.run(client_cmds)
        assert p.returncode == 0
