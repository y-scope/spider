"""Spider client driver module."""

from collections.abc import Sequence
from copy import deepcopy
from uuid import uuid4

import msgpack

from spider_py import core
from spider_py.client.data import Data
from spider_py.client.job import Job
from spider_py.client.task_graph import TaskGraph
from spider_py.storage import MariaDBStorage, parse_jdbc_url
from spider_py.type import to_tdl_type_str
from spider_py.utils import msgpack_encoder


class Driver:
    """Spider client driver class."""

    def __init__(self, storage_url: str) -> None:
        """
        Creates a new Spider client driver and connects to the storage.
        :param storage_url: The URL of the storage to connect to.
        :raises StorageError: If the storage cannot be connected to.
        """
        self.driver_id = uuid4()
        self.storage = MariaDBStorage(parse_jdbc_url(storage_url))

    def submit_jobs(
        self, task_graphs: Sequence[TaskGraph], args: Sequence[Sequence[object]]
    ) -> Sequence[Job]:
        """
        Submits a list of jobs to the storage.
        :param task_graphs: The list of task graphs to submit. Each task graph represents a job.
        :param args: The arguments for each job.
        :return: A sequence of `Job` objects representing the submitted jobs.
        :raises ValueError: If the number of job inputs does not match the number of arguments.
        """
        msg = "Number of job inputs does not match number of arguments"
        if len(task_graphs) != len(args):
            raise ValueError(msg)

        if not task_graphs:
            return []

        core_task_graphs = []
        for task_graph, task_args in zip(task_graphs, args, strict=True):
            core_graph = deepcopy(task_graph._impl) # TODO
            arg_index = 0
            for task in core_graph.tasks:
                task.state = core.TaskState.Pending # TODO
            for task_index in core_graph.input_task_indices:
                task = core_graph.tasks[task_index]
                task.state = core.TaskState.Ready # TODO
                for task_input in task.task_inputs:
                    if arg_index >= len(task_args):
                        raise ValueError(msg)
                    arg = task_args[arg_index]
                    arg_index += 1
                    if isinstance(arg, Data):
                        task_input.type = to_tdl_type_str(Data)
                        task_input.value = arg.data_id
                        continue
                    task_input.type = to_tdl_type_str(type(arg))
                    serialized_value = msgpack_encoder(arg)
                    task_input.value = core.TaskInputValue(msgpack.packb(serialized_value))
            if arg_index != len(task_args):
                raise ValueError(msg)
            core_task_graphs.append(core_graph)

        core_jobs = self.storage.submit_jobs(self.driver_id, core_task_graphs)
        return [Job(core_job, self.storage) for core_job in core_jobs]
