import os
import signal
import subprocess
import time
import uuid
from pathlib import Path

import msgpack
import pytest

from .client import (
    g_storage_url,
    get_task_outputs,
    get_task_state,
    remove_job,
    storage,
    submit_job,
    Task,
    TaskGraph,
    TaskInput,
    TaskOutput,
)
from .utils import g_scheduler_port


def start_scheduler_worker(storage_url: str, scheduler_port: int, lib: str):
    root_dir = Path(__file__).resolve().parents[2]
    bin_dir = root_dir / "src" / "spider"
    popen_opts = dict(stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    scheduler_cmds = [
        str(bin_dir / "spider_scheduler"),
        "--host",
        "127.0.0.1",
        "--port",
        str(scheduler_port),
        "--storage_url",
        storage_url,
    ]
    scheduler_process = subprocess.Popen(scheduler_cmds, **popen_opts)
    worker_cmds = [
        str(bin_dir / "spider_worker"),
        "--host",
        "127.0.0.1",
        "--storage_url",
        storage_url,
        "--libs",
        lib,
    ]
    worker_process = subprocess.Popen(worker_cmds, **popen_opts)

    return scheduler_process, worker_process


@pytest.fixture(scope="function")
def scheduler_worker_signal(storage):
    scheduler_process, worker_process = start_scheduler_worker(
        storage_url=g_storage_url, scheduler_port=g_scheduler_port, lib="tests/libsignal_test.so"
    )
    # Wait for 5 second to make sure the scheduler and worker are started
    time.sleep(5)
    yield scheduler_process, worker_process
    worker_process.kill()
    scheduler_process.kill()


class TestWorkerSignal:

    # Test that worker propagates the SIGTERM signal to the task executor.
    # Submit a task that checks whether the task executor receives the SIGTERM signal.
    # The task should return the SIGTERM signal number as the output.
    # Later task should not be executed.
    # Worker should exit with SIGTERM.
    def test_task_signal(self, storage, scheduler_worker_signal):
        _, worker_process = scheduler_worker_signal

        # Submit signal handler task to check for SIGTERM signal in task executor
        task = Task(
            id=uuid.uuid4(),
            function_name="signal_handler_test",
            inputs=[
                TaskInput(type="int", value=msgpack.packb(0)),
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
        # Sleep for 1 second to wait for the task to start
        time.sleep(1)

        # Check if the task is in progress
        assert get_task_state(storage, task.id) == "running"

        # Send signal to worker
        os.kill(worker_process.pid, signal.SIGTERM)

        # Submit new task
        new_task = Task(
            id=uuid.uuid4(),
            function_name="signal_handler_test",
            inputs=[
                TaskInput(type="int", value=msgpack.packb(0)),
            ],
            outputs=[TaskOutput(type="int")],
        )
        new_graph = TaskGraph(
            id=uuid.uuid4(),
            tasks={new_task.id: new_task},
            dependencies=[],
        )
        new_job_id = uuid.uuid4()
        submit_job(storage, new_job_id, new_graph)

        # Sleep for the signal handler task to finish
        time.sleep(15)

        # Check if the task is finished
        assert get_task_state(storage, task.id) == "success"
        # Check if the task output is correct
        results = get_task_outputs(storage, task.id)
        assert results[0].value == msgpack.packb(signal.SIGTERM)

        # Check if the new task is not executed
        assert get_task_state(storage, new_task.id) == "ready"

        # Check the worker process exited with SIGTERM
        assert worker_process.poll() == signal.SIGTERM + 128

        # Cleanup job
        remove_job(storage, new_job_id)
        remove_job(storage, job_id)

    # Test that worker propagates the SIGTERM signal to the task executor.
    # Task executor exits immediately after receiving the signal.
    # The running task should be marked as failed.
    # The worker should exit with SIGTERM.
    def test_task_exit(self, storage, scheduler_worker_signal):
        _, worker_process = scheduler_worker_signal

        # Submit a task to sleep for 10 seconds
        task = Task(
            id=uuid.uuid4(),
            function_name="sleep_test",
            inputs=[
                TaskInput(type="int", value=msgpack.packb(10)),
            ],
            outputs=[TaskOutput(type="int")],
        )
        graph = TaskGraph(
            id=uuid.uuid4(),
            tasks={task.id: task},
            dependencies=[],
        )
        graph_id = uuid.uuid4()
        submit_job(storage, graph_id, graph)

        # Wait for the task start
        time.sleep(1)

        # Check if the task is running
        assert get_task_state(storage, task.id) == "running"

        # Send signal to worker
        os.kill(worker_process.pid, signal.SIGTERM)

        # Sleep for 3 seconds to wait for the task executor and worker to exit
        time.sleep(3)

        # Check the task fails
        assert get_task_state(storage, task.id) == "fail"
        # Check the worker process exited with SIGTERM
        assert worker_process.poll() == signal.SIGTERM + 128

        # Cleanup job
        remove_job(storage, graph_id)
