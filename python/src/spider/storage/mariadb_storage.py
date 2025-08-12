"""MariaDB Storage module."""

from collections.abc import Sequence
from uuid import uuid4

import mariadb
from typing_extensions import override

from spider import core
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
    """MairaDB Storage class."""

    def __init__(self, params: JdbcParameters) -> None:
        """
        Connects to the MariaDB database.
        :param params: The JDBC parameters for connecting to the database.
        :raises StorageError: If the connection to the database fails.
        """
        try:
            self._conn = mariadb.connect(**params.__dict__)
        except mariadb.Error as e:
            raise StorageError(str(e)) from e

    @override
    def submit_jobs(
        self, driver_id: core.DriverId, task_graphs: Sequence[core.TaskGraph]
    ) -> Sequence[core.JobId]:
        try:
            job_ids = [uuid4() for _ in task_graphs]
            with self._conn.cursor() as cursor:
                cursor.executemany(InsertJob, [(job_id, driver_id) for job_id in job_ids])
                cursor.executemany(
                    InsertTask,
                    [
                        (
                            task.task_id,
                            job_id,
                            task.function_name,
                            task.state.value,
                            task.timeout,
                            task.max_retries,
                        )
                        for job_id, task_graph in zip(job_ids, task_graphs, strict=True)
                        for task in task_graph.tasks.values()
                    ],
                )
                cursor.executemany(
                    InsertTaskDependency,
                    [
                        (parent, child)
                        for task_graph in task_graphs
                        for parent, child in task_graph.dependencies
                    ],
                )
                cursor.executemany(
                    InsertInputTask,
                    [
                        (job_id, task_id, position)
                        for job_id, task_graph in zip(job_ids, task_graphs, strict=True)
                        for position, task_id in enumerate(task_graph.input_tasks)
                    ],
                )
                cursor.executemany(
                    InsertOutputTask,
                    [
                        (job_id, task_id, position)
                        for job_id, task_graph in zip(job_ids, task_graphs, strict=True)
                        for position, task_id in enumerate(task_graph.output_tasks)
                    ],
                )
                cursor.executemany(
                    InsertTaskOutput,
                    [
                        (task.task_id, position, task_output.type)
                        for task_graph in task_graphs
                        for task in task_graph.tasks.values()
                        for position, task_output in enumerate(task.task_outputs)
                    ],
                )
                cursor.executemany(
                    InsertTaskInputData,
                    [
                        (task.task_id, position, task_input.type, task_input.value)
                        for task_graph in task_graphs
                        for task in task_graph.tasks.values()
                        for position, task_input in enumerate(task.task_inputs)
                        if isinstance(task_input.value, core.TaskInputData)
                    ],
                )
                cursor.executemany(
                    InsertTaskInputValue,
                    [
                        (task.task_id, position, task_input.type, task_input.value)
                        for task_graph in task_graphs
                        for task in task_graph.tasks.values()
                        for position, task_input in enumerate(task.task_inputs)
                        if isinstance(task_input.value, core.TaskInputValue)
                    ],
                )
                cursor.executemany(
                    InsertTaskInputOutput,
                    [
                        (
                            task.task_id,
                            position,
                            task_input.type,
                            task_input.value.task_id,
                            task_input.value.position,
                        )
                        for task_graph in task_graphs
                        for task in task_graph.tasks.values()
                        for position, task_input in enumerate(task.task_inputs)
                        if isinstance(task_input.value, core.TaskInputOutput)
                    ],
                )
                cursor.executemany()
                self._conn.commit()
                return job_ids
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e
