"""MariaDB Storage module."""

from collections.abc import Sequence
from uuid import UUID, uuid4

import mariadb
from typing_extensions import override

from spider_py import core
from spider_py.core import get_state_str
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


GetJobStatus = """
SELECT
  `state`
FROM
  `jobs`
WHERE
  `id` = ?"""

GetOutputTasks = """
SELECT
  `task_id`
FROM
  `output_tasks`
WHERE
  `job_id` = ?
ORDER BY
  `position`"""

GetTaskOutputs = """
SELECT
  `type`,
  `value`,
  `data_id`
FROM
  `task_outputs`
WHERE
  `task_id` = ?
ORDER BY
  `position`"""

InsertData = """
INSERT INTO
  `data` (`id`, `value`, `hard_locality`)
VALUES
  (?, ?, ?)"""

InsertDataLocality = """
INSERT INTO
  `data_locality` (`id`, `address`)
VALUES
  (?, ?)"""

InsertDataRefDriver = """
INSERT INTO
  `data_ref_driver` (`id`, `driver_id`)
VALUES
  (?, ?)"""

InsertDataRefTask = """
INSERT INTO
  `data_ref_task` (`id`, `task_id`)
VALUES
  (?, ?)"""

GetData = """
SELECT
  `value`,
  `hard_locality`
FROM
  `data`
WHERE
  `id` = ?"""

GetDataLocality = """
SELECT
  `address`
FROM
  `data_locality`
WHERE
  `id` = ?"""

InsertDriver = """
INSERT INTO
  `drivers` (`id`)
VALUES
  (?)"""


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
                            get_state_str(task.state),
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
                        for position, task_index in enumerate(task_graph.input_tasks)
                    ],
                )
                cursor.executemany(
                    InsertOutputTask,
                    [
                        (job_id.bytes, task_ids[graph_index][task_index].bytes, position)
                        for graph_index, (job_id, task_graph) in enumerate(
                            zip(job_ids, task_graphs, strict=True)
                        )
                        for position, task_index in enumerate(task_graph.output_tasks)
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
                        task_input.value.id.bytes
                        if isinstance(task_input.value, core.Data)
                        else task_input.value.bytes,
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
                return [core.Job(job_id) for job_id in job_ids]
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e

    @override
    def get_job_status(self, job: core.Job) -> core.JobStatus:
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(GetJobStatus, (job.job_id.bytes,))
                row = cursor.fetchone()
                if row is None:
                    msg = f"No job found with id {job.job_id}"
                    raise StorageError(msg)
                status_str = row[0]
                match status_str:
                    case "running":
                        status = core.JobStatus.Running
                    case "success":
                        status = core.JobStatus.Succeeded
                    case "fail":
                        status = core.JobStatus.Failed
                    case "cancel":
                        status = core.JobStatus.Cancelled
                    case _:
                        msg = "Unknown job status"
                        raise StorageError(msg)
                self._conn.commit()
                return status
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e

    @override
    def get_job_results(self, job: core.Job) -> list[core.TaskOutput] | None:
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(GetOutputTasks, (job.job_id.bytes,))
                task_ids = [task_id for (task_id,) in cursor.fetchall()]

                results = []
                for task_id in task_ids:
                    cursor.execute(GetTaskOutputs, (task_id,))
                    for output_type, value, data_id in cursor.fetchall():
                        if value is not None:
                            results.append(
                                core.TaskOutput(
                                    type=output_type,
                                    value=core.TaskOutputValue(value),
                                )
                            )
                        elif data_id is not None:
                            data = self.get_data(core.DataId(data_id))
                            results.append(
                                core.TaskOutput(
                                    type=output_type,
                                    value=data,
                                )
                            )
                        else:
                            self._conn.commit()
                            return None
                self._conn.commit()
                return results
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e

    def _create_data_with_ref(self, data: core.Data, insert_ref: str, ref_id: UUID) -> None:
        """
        Inserts data object into the database with reference.
        :param data: The data object to insert.
        :param insert_ref: The SQL statement to insert the reference.
        :param ref_id: The reference ID (DriverId or TaskId).
        :raises StorageError: If the insertion fails.
        """
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    InsertData,
                    (data.id.bytes, data.value, data.hard_locality),
                )
                if data.localities:
                    cursor.executemany(
                        InsertDataLocality,
                        [(data.id.bytes, locality.address) for locality in data.localities],
                    )
                cursor.execute(
                    insert_ref,
                    (data.id.bytes, ref_id.bytes),
                )
                self._conn.commit()
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e

    @override
    def create_driver_data(self, driver_id: core.DriverId, data: core.Data) -> None:
        self._create_data_with_ref(data, InsertDataRefDriver, driver_id)

    @override
    def create_task_data(self, task_id: core.TaskId, data: core.Data) -> None:
        self._create_data_with_ref(data, InsertDataRefTask, task_id)

    @override
    def get_data(self, data_id: core.DataId) -> core.Data:
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(GetData, (data_id.bytes,))
                row = cursor.fetchone()
                if row is None:
                    msg = f"No data found with id {data_id}"
                    raise StorageError(msg)
                value, hard_locality = row
                data = core.Data(id=data_id, value=value, hard_locality=hard_locality)
                cursor.execute(GetDataLocality, (data_id.bytes,))
                for (address,) in cursor.fetchall():
                    data.localities.append(core.DataLocality(address))
                self._conn.commit()
                return data
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e

    @override
    def create_driver(self, driver_id: core.DriverId) -> None:
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(InsertDriver, (driver_id.bytes,))
                self._conn.commit()
        except mariadb.Error as e:
            self._conn.rollback()
            raise StorageError(str(e)) from e
