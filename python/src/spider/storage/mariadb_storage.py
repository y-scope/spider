"""MariaDB Storage module."""

from collections.abc import Sequence
from uuid import uuid4

import mariadb
from typing_extensions import override

from spider import core
from spider.core import get_state_str
from spider.storage.jdbc_url import JdbcParameters
from spider.storage.storage import Storage, StorageError

InsertJob = """
INSERT INTO
  `jobs` (`id`, `client_id`)
VALUES
  (?, ?)"""

InsertTask = """
INSERT INTO
  `tasks` (`id`, `job_id`, `func_name`, `state`, `timeout`, `max_retry`)
VALUES
  (?, ?, ?, ?, ?, ?)"""

InsertTaskInputOutput = """
INSERT INTO
  `task_inputs` (`task_id`, `position`, `type`, `output_task_id`, `output_task_position`)
VALUES
  (?, ?, ?, ?, ?)"""

InsertTaskInputData = """
INSERT INTO
  `task_inputs` (`task_id`, `position`, `type`, `data_id`)
VALUES
  (?, ?, ?, ?)"""

InsertTaskInputValue = """
INSERT INTO
  `task_inputs` (`task_id`, `position`, `type`, `value`)
VALUES
  (?, ?, ?, ?)"""

InsertTaskOutput = """
INSERT INTO
  `task_outputs` (`task_id`, `position`, `type`)
VALUES
  (?, ?, ?)"""

InsertTaskDependency = """
INSERT INTO
  `task_dependencies` (parent, child)
VALUES
  (?, ?)"""

InsertInputTask = """
INSERT INTO
  `input_tasks` (`job_id`, `task_id`, `position`)
VALUES
  (?, ?, ?)"""

InsertOutputTask = """
INSERT INTO
  `output_tasks` (`job_id`, `task_id`, `position`)
VALUES
  (?, ?, ?)"""


class MariaDBStorage(Storage):
    """MariaDB Storage class."""

    def __init__(self, params: JdbcParameters) -> None:
        """
        Connects to the MariaDB database.
        :param params: The JDBC parameters for connecting to the database.
        :raises StorageError: If the connection to the database fails.
        """
        try:
            self._conn = mariadb.connect(
                host=params.host,
                port=params.port,
                user=params.user,
                password=params.password,
                database=params.database,
            )
        except mariadb.Error as e:
            raise StorageError(str(e)) from e

    @override
    def submit_jobs(
        self, driver_id: core.DriverId, task_graphs: Sequence[core.TaskGraph]
    ) -> Sequence[core.Job]:
        if not task_graphs:
            return []
        try:
            job_ids = [uuid4() for _ in task_graphs]
            with self._conn.cursor() as cursor:
                cursor.executemany(
                    InsertJob, [(job_id.bytes, driver_id.bytes) for job_id in job_ids]
                )
                cursor.executemany(
                    InsertTask,
                    [
                        (
                            task.task_id.bytes,
                            job_id.bytes,
                            task.function_name,
                            get_state_str(task.state),
                            task.timeout,
                            task.max_retries,
                        )
                        for job_id, task_graph in zip(job_ids, task_graphs, strict=True)
                        for task in task_graph.tasks.values()
                    ],
                )
                dep_params = [
                    (parent.bytes, child.bytes)
                    for task_graph in task_graphs
                    for parent, child in task_graph.dependencies
                ]
                if dep_params:
                    cursor.executemany(
                        InsertTaskDependency,
                        dep_params,
                    )
                cursor.executemany(
                    InsertInputTask,
                    [
                        (job_id.bytes, task_id.bytes, position)
                        for job_id, task_graph in zip(job_ids, task_graphs, strict=True)
                        for position, task_id in enumerate(task_graph.input_tasks)
                    ],
                )
                cursor.executemany(
                    InsertOutputTask,
                    [
                        (job_id.bytes, task_id.bytes, position)
                        for job_id, task_graph in zip(job_ids, task_graphs, strict=True)
                        for position, task_id in enumerate(task_graph.output_tasks)
                    ],
                )
                cursor.executemany(
                    InsertTaskOutput,
                    [
                        (task.task_id.bytes, position, task_output.type)
                        for task_graph in task_graphs
                        for task in task_graph.tasks.values()
                        for position, task_output in enumerate(task.task_outputs)
                    ],
                )
                input_data_params = [
                    (task.task_id.bytes, position, task_input.type, task_input.value.bytes)
                    for task_graph in task_graphs
                    for task in task_graph.tasks.values()
                    for position, task_input in enumerate(task.task_inputs)
                    if isinstance(task_input.value, core.TaskInputData)
                ]
                if input_data_params:
                    cursor.executemany(
                        InsertTaskInputData,
                        input_data_params,
                    )
                input_value_params = [
                    (task.task_id.bytes, position, task_input.type, task_input.value)
                    for task_graph in task_graphs
                    for task in task_graph.tasks.values()
                    for position, task_input in enumerate(task.task_inputs)
                    if isinstance(task_input.value, core.TaskInputValue)
                ]
                if input_value_params:
                    cursor.executemany(
                        InsertTaskInputValue,
                        input_value_params,
                    )
                input_output_params = [
                    (
                        task.task_id.bytes,
                        position,
                        task_input.type,
                        task_input.value.task_id.bytes,
                        task_input.value.position,
                    )
                    for task_graph in task_graphs
                    for task in task_graph.tasks.values()
                    for position, task_input in enumerate(task.task_inputs)
                    if isinstance(task_input.value, core.TaskInputOutput)
                ]
                if input_output_params:
                    cursor.executemany(
                        InsertTaskInputOutput,
                        input_output_params,
                    )
                self._conn.commit()
                return [core.Job(job_id) for job_id in job_ids]
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e
