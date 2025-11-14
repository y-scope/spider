"""Spider client driver module."""

from collections.abc import Sequence
from uuid import uuid4

import msgpack

from spider_py import core
from spider_py.client.data import Data
from spider_py.client.job import Job
from spider_py.client.task_graph import TaskGraph
from spider_py.storage import MariaDBStorage, parse_jdbc_url
from spider_py.type import parse_tdl_type
from spider_py.utils.serde import to_serializable

_argument_number_mismatch_msg = "Number of job inputs does not match number of arguments."


class Driver:
    """Spider client driver class."""

    def __init__(self, storage_url: str) -> None:
        """
        Creates a new Spider client driver and connects to the storage.
        :param storage_url: The URL of the storage to connect to.
        :raises StorageError: If the storage cannot be connected to.
        """
        self._driver_id = uuid4()
        self._storage = MariaDBStorage(parse_jdbc_url(storage_url))
        self._storage.create_driver(self._driver_id)

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
        if len(task_graphs) != len(args):
            raise ValueError(_argument_number_mismatch_msg)

        if not task_graphs:
            return []

        core_task_graphs = []
        for task_graph, task_args in zip(task_graphs, args, strict=True):
            core_task_graphs.append(_copy_graph_for_submission(task_graph, task_args))

        core_jobs = self._storage.submit_jobs(self._driver_id, core_task_graphs)
        return [Job(core_job, self._storage) for core_job in core_jobs]

    def create_data(self, data: Data) -> None:
        """
        Creates a data in the storage.
        :param data:
        """
        self._storage.create_data_with_driver_ref(self._driver_id, data._impl)


def _copy_graph_for_submission(
    task_graph: TaskGraph, task_args: Sequence[object]
) -> core.TaskGraph:
    """
    Copies the given task graph for submission by setting task states and assigning input values.
    :param task_graph: The task graph to copy.
    :param task_args: The arguments to assign to the input tasks.
    :return: A copied core task graph ready for submission.
    """
    core_graph = task_graph._impl.copy()
    task_args_it = iter(task_args)
    next_arg = next(task_args_it, None)
    for task in core_graph.tasks:
        task.set_pending()
    for task_index in core_graph.input_task_indices:
        task = core_graph.tasks[task_index]
        task.set_ready()
        for task_input in task.task_inputs:
            if next_arg is None:
                raise ValueError(_argument_number_mismatch_msg)
            arg = next_arg
            next_arg = next(task_args_it, None)
            if isinstance(arg, Data):
                task_input.value = arg.id
                continue
            native_type = parse_tdl_type(task_input.type).native_type()
            serialized_value = to_serializable(arg, native_type)
            task_input.value = core.TaskInputValue(msgpack.packb(serialized_value))
    if next_arg is not None:
        raise ValueError(_argument_number_mismatch_msg)
    return core_graph
