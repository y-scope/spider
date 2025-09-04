"""MariaDB Storage module."""

from collections.abc import Sequence
from uuid import uuid4

import mariadb
from typing_extensions import override

from spider_py import core
from spider_py.storage.jdbc_url import JdbcParameters
from spider_py.storage.storage import Storage, StorageError

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
    ) -> Sequence[core.JobId]:
        if not task_graphs:
            return []
        try:
            job_ids = [uuid4() for _ in task_graphs]

            task_ids = [[uuid4() for _ in graph.tasks] for graph in task_graphs]

            with self._conn.cursor() as cursor:
                cursor.executemany(
                    InsertJob, [(job_id.bytes, driver_id.bytes) for job_id in job_ids]
                )
                cursor.executemany(
                    InsertTask,
                    [
                        (
                            task_ids[graph_index][task_index].bytes,
                            job_id.bytes,
                            task.function_name,
                            task.state.get_state_str(),
                            task.timeout,
                            task.max_retries,
                        )
                        for graph_index, (job_id, task_graph) in enumerate(
                            zip(job_ids, task_graphs, strict=True)
                        )
                        for task_index, task in enumerate(task_graph.tasks)
                    ],
                )
                dep_params = [
                    (task_ids[graph_index][parent].bytes, task_ids[graph_index][child].bytes)
                    for graph_index, task_graph in enumerate(task_graphs)
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
                        (job_id.bytes, task_ids[graph_index][task_index].bytes, position)
                        for graph_index, (job_id, task_graph) in enumerate(
                            zip(job_ids, task_graphs, strict=True)
                        )
                        for position, task_index in enumerate(task_graph.input_task_indices)
                    ],
                )
                cursor.executemany(
                    InsertOutputTask,
                    [
                        (job_id.bytes, task_ids[graph_index][task_index].bytes, position)
                        for graph_index, (job_id, task_graph) in enumerate(
                            zip(job_ids, task_graphs, strict=True)
                        )
                        for position, task_index in enumerate(task_graph.output_task_indices)
                    ],
                )
                cursor.executemany(
                    InsertTaskOutput,
                    [
                        (task_ids[graph_index][task_index].bytes, position, task_output.type)
                        for graph_index, task_graph in enumerate(task_graphs)
                        for task_index, task in enumerate(task_graph.tasks)
                        for position, task_output in enumerate(task.task_outputs)
                    ],
                )
                input_data_params = [
                    (
                        task_ids[graph_index][task_index].bytes,
                        position,
                        task_input.type,
                        task_input.value.bytes,
                    )
                    for graph_index, task_graph in enumerate(task_graphs)
                    for task_index, task in enumerate(task_graph.tasks)
                    for position, task_input in enumerate(task.task_inputs)
                    if isinstance(task_input.value, core.TaskInputData)
                ]
                if input_data_params:
                    cursor.executemany(
                        InsertTaskInputData,
                        input_data_params,
                    )
                input_value_params = [
                    (
                        task_ids[graph_index][task_index].bytes,
                        position,
                        task_input.type,
                        task_input.value,
                    )
                    for graph_index, task_graph in enumerate(task_graphs)
                    for task_index, task in enumerate(task_graph.tasks)
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
                        task_ids[graph_index][input_task_index].bytes,
                        input_task_position,
                        task_graph.tasks[input_task_index].task_inputs[input_task_position].type,
                        task_ids[graph_index][output_task_index].bytes,
                        output_task_position,
                    )
                    for graph_index, task_graph in enumerate(task_graphs)
                    for (
                        input_task_index,
                        input_task_position,
                        output_task_index,
                        output_task_position,
                    ) in task_graph.task_input_output_refs
                ]
                if input_output_params:
                    cursor.executemany(
                        InsertTaskInputOutput,
                        input_output_params,
                    )
                self._conn.commit()
                return job_ids
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e
