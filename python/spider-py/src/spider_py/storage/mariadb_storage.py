"""MariaDB Storage module."""

from collections.abc import Sequence
from uuid import UUID, uuid4

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
                # Insert jobs table
                cursor.executemany(
                    InsertJob, [(job_id.bytes, driver_id.bytes) for job_id in job_ids]
                )
                # Insert tasks table
                cursor.executemany(
                    InsertTask,
                    self._gen_task_insertion_params(job_ids, task_ids, task_graphs),
                )

                # Insert task dependencies table
                dep_params = self._gen_task_dependency_insertion_params(task_ids, task_graphs)
                if dep_params:
                    cursor.executemany(
                        InsertTaskDependency,
                        dep_params,
                    )

                # Insert input tasks table
                cursor.executemany(
                    InsertInputTask,
                    self._gen_input_task_insertion_params(job_ids, task_ids, task_graphs),
                )

                # Insert output tasks table
                cursor.executemany(
                    InsertOutputTask,
                    self._gen_output_task_insertion_params(job_ids, task_ids, task_graphs),
                )

                # Insert task outputs table
                cursor.executemany(
                    InsertTaskOutput,
                    self._gen_task_output_insertion_params(task_ids, task_graphs),
                )

                # Insert task input data table
                input_data_params = self._gen_task_input_data_insertion_params(
                    task_ids, task_graphs
                )
                if input_data_params:
                    cursor.executemany(
                        InsertTaskInputData,
                        input_data_params,
                    )

                # Insert task input values table
                input_value_params = self._gen_task_input_value_insertion_params(
                    task_ids, task_graphs
                )
                if input_value_params:
                    cursor.executemany(
                        InsertTaskInputValue,
                        input_value_params,
                    )

                # Insert task input outputs table
                input_output_params = self._gen_task_input_output_ref_insertion_params(
                    task_ids, task_graphs
                )
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

    @staticmethod
    def _gen_task_insertion_params(
        job_ids: Sequence[core.JobId],
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, bytes, str, str, float, int]]:
        """
        Generates parameters for inserting tasks into the database.
        :param job_ids: The job IDs.
        :param task_ids: The task IDs. Must be the same length as `job_ids`.
        :param task_graphs: The task graphs. Must be the same length as `job_ids`.
        :return: A list of tuples containing the parameters for each task. Each tuple contains:
            - Task ID.
            - Job ID.
            - Task function name.
            - Task state.
            - Task timeout.
            - Task max retry.
        """
        task_insert_params = []
        for graph_index, (job_id, task_graph) in enumerate(zip(job_ids, task_graphs, strict=True)):
            for task_index, task in enumerate(task_graph.tasks):
                task_insert_params.append(
                    (
                        task_ids[graph_index][task_index].bytes,
                        job_id.bytes,
                        task.function_name,
                        task.state.get_state_str(),
                        task.timeout,
                        task.max_retries,
                    )
                )
        return task_insert_params

    @staticmethod
    def _gen_task_dependency_insertion_params(
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, bytes]]:
        """
        Generates parameters for inserting task dependencies into the database.
        :param task_ids: The task IDs.
        :param task_graphs: The task graphs. Must be the same length as `task_ids`.
        :return: A list of tuples containing the parameters for each task dependency. Each tuple
            contains:
            - Parent task ID.
            - Child task ID.
        """
        dep_params = []
        for graph_index, task_graph in enumerate(task_graphs):
            for parent, child in task_graph.dependencies:
                dep_params.append(
                    (task_ids[graph_index][parent].bytes, task_ids[graph_index][child].bytes)
                )
        return dep_params

    @staticmethod
    def _gen_input_task_insertion_params(
        job_ids: Sequence[core.JobId],
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, bytes, int]]:
        """
        Generates parameters for inserting input tasks into the database.
        :param job_ids: The job IDs.
        :param task_ids: The task IDs. Must be the same length as `job_ids`.
        :param task_graphs: The task graphs. Must be the same length as `job_ids`.
        :return: A list of tuples containing the parameters for each input task. Each tuple
            contains:
            - Job ID.
            - Task ID.
            - The positional index of the input task.
        """
        input_task_params = []
        for graph_index, (job_id, task_graph) in enumerate(zip(job_ids, task_graphs, strict=True)):
            for position, task_index in enumerate(task_graph.input_task_indices):
                input_task_params.append(
                    (job_id.bytes, task_ids[graph_index][task_index].bytes, position)
                )
        return input_task_params

    @staticmethod
    def _gen_output_task_insertion_params(
        job_ids: Sequence[core.JobId],
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, bytes, int]]:
        """
        Generates parameters for inserting output tasks into the database.
        :param job_ids: The job IDs.
        :param task_ids: The task IDs. Must be the same length as `job_ids`.
        :param task_graphs: The task graphs. Must be the same length as `job_ids`.
        :return: A list of tuples containing the parameters for each output task. Each tuple
            contains:
            - Job ID.
            - Task ID.
            - The positional index of the output task.
        """
        output_task_params = []
        for graph_index, (job_id, task_graph) in enumerate(zip(job_ids, task_graphs, strict=True)):
            for position, task_index in enumerate(task_graph.output_task_indices):
                output_task_params.append(
                    (job_id.bytes, task_ids[graph_index][task_index].bytes, position)
                )
        return output_task_params

    @staticmethod
    def _gen_task_output_insertion_params(
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, int, str]]:
        """
        Generates parameters for inserting task outputs into the database.
        :param task_ids: The task IDs.
        :param task_graphs: The task graphs. Must be the same length as `task_ids`.
        :return: A list of tuples containing the parameters for each task output. Each tuple
            contains:
            - Task ID.
            - Positional index of the output.
            - Type of the output.
        """
        output_params = []
        for graph_index, task_graph in enumerate(task_graphs):
            for task_index, task in enumerate(task_graph.tasks):
                for position, task_output in enumerate(task.task_outputs):
                    output_params.append(
                        (task_ids[graph_index][task_index].bytes, position, task_output.type)
                    )
        return output_params

    @staticmethod
    def _gen_task_input_data_insertion_params(
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, int, str, bytes]]:
        """
        Generates parameters for inserting task input data into the database.
        :param task_ids: The task IDs.
        :param task_graphs: The task graphs. Must be the same length as `task_ids`.
        :return: A list of tuples containing the parameters for each task input data. Each tuple
            contains:
            - Task ID.
            - Positional index of the input.
            - Type of the input.
            - Input data.
        """
        input_data_params = []
        for graph_index, task_graph in enumerate(task_graphs):
            for task_index, task in enumerate(task_graph.tasks):
                for position, task_input in enumerate(task.task_inputs):
                    if isinstance(task_input.value, core.TaskInputData):
                        input_data_params.append(
                            (
                                task_ids[graph_index][task_index].bytes,
                                position,
                                task_input.type,
                                task_input.value.bytes,
                            )
                        )
        return input_data_params

    @staticmethod
    def _gen_task_input_value_insertion_params(
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, int, str, bytes]]:
        """
        Generates parameters for inserting task input values into the database.
        :param task_ids: The task IDs.
        :param task_graphs: The task graphs. Must be the same length as `task_ids`.
        :return: A list of tuples containing the parameters for each task input value. Each tuple
            contains:
            - Task ID.
            - Positional index of the input.
            - Type of the input.
            - Input value.
        """
        input_value_params = []
        for graph_index, task_graph in enumerate(task_graphs):
            for task_index, task in enumerate(task_graph.tasks):
                for position, task_input in enumerate(task.task_inputs):
                    if isinstance(task_input.value, core.TaskInputValue):
                        input_value_params.append(
                            (
                                task_ids[graph_index][task_index].bytes,
                                position,
                                task_input.type,
                                task_input.value,
                            )
                        )
        return input_value_params

    @staticmethod
    def _gen_task_input_output_ref_insertion_params(
        task_ids: Sequence[Sequence[UUID]],
        task_graphs: Sequence[core.TaskGraph],
    ) -> list[tuple[bytes, int, str, bytes, int]]:
        """
        Generates parameters for inserting task input output refs into the database.
        :param task_ids: The task IDs.
        :param task_graphs: The task graphs. Must be the same length as `task_ids`.
        :return: A list of tuples containing the parameters for each task input output ref. Each
            tuple contains:
            - Input task ID.
            - Positional index of the input.
            - Type of the input.
            - Output task ID.
            - Positional index of the output.
        """
        input_output_params = []
        for graph_index, task_graph in enumerate(task_graphs):
            for input_output_ref in task_graph.task_input_output_refs:
                input_output_params.append(
                    (
                        task_ids[graph_index][input_output_ref.input_task_index].bytes,
                        input_output_ref.input_position,
                        task_graph.tasks[input_output_ref.input_task_index]
                        .task_inputs[input_output_ref.input_position]
                        .type,
                        task_ids[graph_index][input_output_ref.output_task_index].bytes,
                        input_output_ref.output_position,
                    )
                )
        return input_output_params
