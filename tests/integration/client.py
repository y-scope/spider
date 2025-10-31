"""Simple Spider client for testing purposes."""

import os
import re
import uuid
from collections.abc import Generator
from dataclasses import dataclass
from typing import cast

import mysql.connector
import pytest


@dataclass
class TaskInput:
    """
    Represents an input to a task.
    It can be a direct value, a reference to another task's output, or a reference to data.
    """

    type: str
    task_output: tuple[uuid.UUID, int] | None = None
    value: bytes | None = None
    data_id: uuid.UUID | None = None


@dataclass
class TaskOutput:
    """
    Represents an output of a task.
    It can either be a direct value or a reference to data.
    """

    type: str
    value: bytes | None = None
    data_id: uuid.UUID | None = None


@dataclass
class Task:
    """Represents a unit of work in the task graph."""

    id: uuid.UUID
    function_name: str
    inputs: list[TaskInput]
    outputs: list[TaskOutput]
    timeout: float = 0.0
    max_retries: int = 0


@dataclass
class TaskGraph:
    """Represents a directed acyclic graph of tasks."""

    id: uuid.UUID
    tasks: dict[uuid.UUID, Task]
    dependencies: list[tuple[uuid.UUID, uuid.UUID]]


@dataclass
class Driver:
    """Represents a client that can submit jobs to the task graph."""

    id: uuid.UUID


@dataclass
class Data:
    """Represents a Spider Data object."""

    id: uuid.UUID
    value: bytes


SQLConnection = (
    mysql.connector.abstracts.MySQLConnectionAbstract
    | mysql.connector.pooling.PooledMySQLConnection
)


def create_connection(storage_url: str) -> SQLConnection:
    """
    Creates a MariaDB connection from a JDBC URL.
    :param storage_url: JDBC URL for the MariaDB database.
    :return: The created MySQL connection.
    """
    pattern = re.compile(
        r"jdbc:mariadb://(?P<host>[^:/]+):(?P<port>\d+)/(?P<database>[^?]+)\?user=(?P<user>[^&]+)&password=(?P<password>[^&]+)"
    )
    match = pattern.match(storage_url)
    if not match:
        raise ValueError(storage_url)

    connection_params = match.groupdict()
    return mysql.connector.connect(
        host=connection_params["host"],
        port=int(connection_params["port"]),
        database=connection_params["database"],
        user=connection_params["user"],
        password=connection_params["password"],
    )


def is_head_task(task_id: uuid.UUID, dependencies: list[tuple[uuid.UUID, uuid.UUID]]) -> bool:
    """
    Checks whether the task is a head task. A head task has no parent.
    :param task_id: The ID of the task to check.
    :param dependencies: A list of dependencies of the given task, where each dependency is a pair
        of (parent ID, child ID).
    :return: Whether the task has no parent.
    """
    return not any(dependency[1] == task_id for dependency in dependencies)


g_storage_url = os.getenv(
    "SPIDER_STORAGE_URL",
    "jdbc:mariadb://localhost:3306/spider-storage?user=spider&password=password",
)


@pytest.fixture(scope="session")
def storage() -> Generator[SQLConnection, None, None]:
    """
    Fixture to create a database connection for the test session.
    Yields the connection object for use in tests and ensures the connection is properly closed
    after the test session is complete.
    :return: A generator yielding a MySQL connection object.
    """
    conn = create_connection(g_storage_url)
    yield conn
    conn.close()


def submit_job(conn: SQLConnection, client_id: uuid.UUID, graph: TaskGraph) -> None:
    """
    Submits a job to the database.
    :param conn: The database connection.
    :param client_id: The client ID of the driver submitting the job.
    :param graph: The task graph to be submitted.
    """
    cursor = conn.cursor()

    cursor.execute(
        "INSERT INTO jobs (id, client_id) VALUES (%s, %s)", (graph.id.bytes, client_id.bytes)
    )

    for task_id, task in graph.tasks.items():
        state = "ready" if is_head_task(task_id, graph.dependencies) else "pending"
        cursor.execute(
            "INSERT INTO tasks (id, job_id, func_name, state, timeout, max_retry)"
            " VALUES (%s, %s, %s, %s, %s, %s)",
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
                "INSERT INTO task_inputs (type, task_id, position, output_task_id,"
                " output_task_position, value, data_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
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


def get_task_outputs(conn: SQLConnection, task_id: uuid.UUID) -> list[TaskOutput]:
    """
    Gets the outputs of a task by its ID.
    :param conn: The database connection.
    :param task_id: The ID of the task whose outputs are to be retrieved.
    :return: A list of TaskOutput objects representing the outputs of the task.
    """
    cursor = conn.cursor()

    cursor.execute(
        "SELECT type, value, data_id FROM task_outputs WHERE task_id = %s ORDER BY position",
        (task_id.bytes,),
    )
    outputs = []
    rows = cast("list[tuple[str, bytes | None, bytes | None]]", cursor.fetchall())
    for output_type, value, data_id in rows:
        if value is not None:
            outputs.append(TaskOutput(type=output_type, value=value))
        elif data_id is not None:
            outputs.append(TaskOutput(type=output_type, data_id=uuid.UUID(bytes=data_id)))
        else:
            outputs.append(TaskOutput(type=output_type))

    conn.commit()
    cursor.close()
    return outputs


def get_task_state(conn: SQLConnection, task_id: uuid.UUID) -> str:
    """
    Gets the state of a task by its ID.
    :param conn: The database connection.
    :param task_id: The ID of the task whose state is to be retrieved.
    :return: The state of the task as a string.
    """
    cursor = conn.cursor()

    cursor.execute("SELECT state FROM tasks WHERE id = %s", (task_id.bytes,))
    state = cast("tuple[str]", cursor.fetchone())[0]

    conn.commit()
    cursor.close()
    return state


def remove_job(conn: SQLConnection, job_id: uuid.UUID) -> None:
    """
    Removes a job from the database by its ID.
    :param conn: The database connection.
    :param job_id: The ID of the job to be removed.
    """
    cursor = conn.cursor()

    cursor.execute("DELETE FROM jobs WHERE id = %s", (job_id.bytes,))
    conn.commit()
    cursor.close()


def add_driver(conn: SQLConnection, driver: Driver) -> None:
    """
    Adds a new driver into the database.
    :param conn: The database connection.
    :param driver: The driver object to be registered.
    """
    cursor = conn.cursor()

    cursor.execute("INSERT INTO drivers (id) VALUES (%s)", (driver.id.bytes,))

    conn.commit()
    cursor.close()


def add_driver_data(conn: SQLConnection, driver: Driver, data: Data) -> None:
    """
    Adds new data for a driver in the database.
    :param conn: The database connection.
    :param driver: The driver object to which the data is associated.
    :param data: The data object to be added.
    """
    cursor = conn.cursor()

    cursor.execute("INSERT INTO data (id, value) VALUES (%s, %s)", (data.id.bytes, data.value))
    cursor.execute(
        "INSERT INTO data_ref_driver (driver_id, id) VALUES (%s, %s)",
        (driver.id.bytes, data.id.bytes),
    )

    conn.commit()
    cursor.close()


def remove_data(conn: SQLConnection, data: Data) -> None:
    """
    Removes data from the database by its ID.
    :param conn: The database connection.
    :param data: The data object to be removed.
    """
    cursor = conn.cursor()

    cursor.execute("DELETE FROM data WHERE id = %s", (data.id.bytes,))
    conn.commit()
    cursor.close()
