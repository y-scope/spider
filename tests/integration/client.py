import re
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import mysql.connector
import pytest


@dataclass
class TaskInput:
    type: str
    task_output: Optional[Tuple[uuid.UUID, int]] = None
    value: Optional[str] = None
    data_id: Optional[uuid.UUID] = None


@dataclass
class TaskOutput:
    type: str
    value: Optional[str] = None
    data_id: Optional[uuid.UUID] = None


@dataclass
class Task:
    id: uuid.UUID
    function_name: str
    inputs: List[TaskInput]
    outputs: List[TaskOutput]
    timeout: float = 0.0
    max_retries: int = 0


@dataclass
class TaskGraph:
    id: uuid.UUID
    tasks: Dict[uuid.UUID, Task]
    dependencies: List[Tuple[uuid.UUID, uuid.UUID]]


@dataclass
class Driver:
    id: uuid.UUID
    addr: str


@dataclass
class Data:
    id: uuid.UUID
    value: str


def create_connection(storage_url: str):
    pattern = re.compile(
        r"jdbc:mariadb://(?P<host>[^:/]+):(?P<port>\d+)/(?P<database>[^?]+)\?user=(?P<user>[^&]+)&password=(?P<password>[^&]+)"
    )
    match = pattern.match(storage_url)
    if not match:
        raise ValueError("Invalid JDBC URL format")

    connection_params = match.groupdict()
    return mysql.connector.connect(
        host=connection_params["host"],
        port=int(connection_params["port"]),
        database=connection_params["database"],
        user=connection_params["user"],
        password=connection_params["password"],
    )


def is_head_task(task_id: uuid.UUID, dependencies: List[Tuple[uuid.UUID, uuid.UUID]]):
    return not any(dependency[1] == task_id for dependency in dependencies)


storage_url = "jdbc:mariadb://localhost:3306/spider_test?user=root&password=password"


@pytest.fixture(scope="session")
def storage():
    conn = create_connection(storage_url)
    yield conn
    conn.close()


def submit_job(conn, client_id: uuid.UUID, graph: TaskGraph):
    cursor = conn.cursor()

    cursor.execute(
        "INSERT INTO jobs (id, client_id) VALUES (%s, %s)", (graph.id.bytes, client_id.bytes)
    )

    for task_id, task in graph.tasks.items():
        if is_head_task(task_id, graph.dependencies):
            state = "ready"
        else:
            state = "pending"
        cursor.execute(
            "INSERT INTO tasks (id, job_id, func_name, state, timeout, max_retry) VALUES (%s, %s, %s, %s, %s, %s)",
            (
                task.id.bytes,
                graph.id.bytes,
                task.function_name,
                state,
                task.timeout,
                task.max_retries,
            ),
        )

        for i, task_input in enumerate(task.inputs):
            cursor.execute(
                "INSERT INTO task_inputs (type, task_id, position, output_task_id, output_task_position, value, data_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    task_input.type,
                    task.id.bytes,
                    i,
                    task_input.task_output[0].bytes if task_input.task_output is not None else None,
                    task_input.task_output[1] if task_input.task_output is not None else None,
                    task_input.value,
                    task_input.data_id.bytes if task_input.data_id is not None else None,
                ),
            )

        for i, task_output in enumerate(task.outputs):
            cursor.execute(
                "INSERT INTO task_outputs (task_id, position, type) VALUES (%s, %s, %s)",
                (task.id.bytes, i, task_output.type),
            )

    for dependency in graph.dependencies:
        cursor.execute(
            "INSERT INTO task_dependencies (parent, child) VALUES (%s, %s)",
            (dependency[0].bytes, dependency[1].bytes),
        )

    conn.commit()
    cursor.close()


def get_task_outputs(conn, task_id: uuid.UUID) -> List[TaskOutput]:
    cursor = conn.cursor()

    cursor.execute(
        "SELECT type, value, data_id FROM task_outputs WHERE task_id = %s ORDER BY position",
        (task_id.bytes,),
    )
    outputs = []
    for output_type, value, data_id in cursor.fetchall():
        if value is not None:
            outputs.append(TaskOutput(type=output_type, value=value))
        elif data_id is not None:
            outputs.append(TaskOutput(type=output_type, data_id=uuid.UUID(bytes=data_id)))
        else:
            outputs.append(TaskOutput(type=output_type))

    conn.commit()
    cursor.close()
    return outputs


def get_task_state(conn, task_id: uuid.UUID) -> str:
    cursor = conn.cursor()

    cursor.execute("SELECT state FROM tasks WHERE id = %s", (task_id.bytes,))
    state = cursor.fetchone()[0]

    conn.commit()
    cursor.close()
    return state


def remove_job(conn, job_id: uuid.UUID):
    cursor = conn.cursor()

    cursor.execute("DELETE FROM jobs WHERE id = %s", (job_id.bytes,))
    conn.commit()
    cursor.close()


def add_driver(conn, driver: Driver):
    cursor = conn.cursor()

    cursor.execute(
        "INSERT INTO drivers (id, address) VALUES (%s, %s)", (driver.id.bytes, driver.addr)
    )

    conn.commit()
    cursor.close()


def add_driver_data(conn, driver: Driver, data: Data):
    cursor = conn.cursor()

    cursor.execute("INSERT INTO data (id, value) VALUES (%s, %s)", (data.id.bytes, data.value))
    cursor.execute(
        "INSERT INTO data_ref_driver (driver_id, id) VALUES (%s, %s)",
        (driver.id.bytes, data.id.bytes),
    )

    conn.commit()
    cursor.close()


def remove_data(conn, data: Data):
    cursor = conn.cursor()

    cursor.execute("DELETE FROM data WHERE id = %s", (data.id.bytes,))
    conn.commit()
    cursor.close()
