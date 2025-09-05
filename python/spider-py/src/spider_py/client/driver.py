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
        self, graphs: Sequence[TaskGraph], args: Sequence[Sequence[object]]
    ) -> Sequence[Job]:
        """
        Submits a list of jobs to the storage.
        :param graphs: The list of task graphs to submit.
        :param args: The arguments for each job.
        :return: A sequence of Job objects representing the submitted jobs.
        :raises StorageError: If the jobs cannot be submitted to the storage.
        :raises ValueError: If the number of job inputs does not match the number of arguments.
        :raises TypeError: If the arguments are not of the expected type.
        """
        msg = "Number of job inputs does not match number of arguments"
        if len(graphs) != len(args):
            raise ValueError(msg)

        if not graphs:
            return []
        task_graphs = []
        for task_graph, task_args in zip(graphs, args, strict=True):
            graph = deepcopy(task_graph._impl)
            arg_index = 0
            for task_index in graph.input_task_indices:
                task = graph.tasks[task_index]
                for task_input in task.task_inputs:
                    if arg_index >= len(task_args):
                        raise ValueError(msg)
                    arg = task_args[arg_index]
                    if isinstance(arg, Data):
                        task_input.type = to_tdl_type_str(Data)
                        task_input.value = arg.data_id
                    else:
                        task_input.type = to_tdl_type_str(type(arg))
                        task_input.value = core.TaskInputValue(msgpack.packb(arg))
                    arg_index += 1
            if arg_index != len(task_args):
                raise ValueError(msg)
            task_graphs.append(graph)

        jobs = self.storage.submit_jobs(self.driver_id, task_graphs)
        return [Job(job, self.storage) for job in jobs]
