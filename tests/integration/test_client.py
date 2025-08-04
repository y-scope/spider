"""Integration test for the client_test C++ program."""

import subprocess
import time
from collections.abc import Generator
from pathlib import Path

import mysql.connector
import pytest

from .client import (
    g_storage_url,
)
from .utils import g_scheduler_port


def start_scheduler_workers(
    storage_url: str, scheduler_port: int
) -> tuple[subprocess.Popen, subprocess.Popen, subprocess.Popen]:
    """
    Start the scheduler and two worker processes.
    :param storage_url:
    :param scheduler_port: The port for the scheduler to listen on.
    :return: scheduler_process, worker_process_0, worker_process_1
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
    worker_process_0 = subprocess.Popen(worker_cmds)
    worker_process_1 = subprocess.Popen(worker_cmds)
    return scheduler_process, worker_process_0, worker_process_1


@pytest.fixture(scope="class")
def scheduler_worker(
        storage: Generator[mysql.connector.MySQLConnection, None, None]
) -> Generator[None, None, None]:
    """
    Fixture to start the scheduler and two worker processes. Yields control to the test class,
    and then kills the processes after the test class is done.
    :return:
    """
    _ = storage  # Avoid ARG001
    scheduler_process, worker_process_0, worker_process_1 = start_scheduler_workers(
        storage_url=g_storage_url, scheduler_port=g_scheduler_port
    )
    # Wait for 5 second to make sure the scheduler and worker are started
    time.sleep(5)
    yield
    scheduler_process.kill()
    worker_process_0.kill()
    worker_process_1.kill()


class TestClient:
    """Test class for the client_test C++ program."""

    @pytest.mark.usefixtures("scheduler_worker")
    def test_client(self) -> None:
        """
        Test the client_test C++ program and check for successful execution.
        :return: None
        """
        dir_path = Path(__file__).resolve().parent
        dir_path = dir_path / ".."
        client_cmds = [
            str(dir_path / "client_test"),
            "--storage_url",
            g_storage_url,
        ]
        p = subprocess.run(client_cmds, check=True, timeout=20)
        assert p.returncode == 0
