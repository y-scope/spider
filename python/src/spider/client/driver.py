"""Spider client driver module."""

from collections.abc import Sequence
from uuid import uuid4

import msgpack

from spider import core
from spider.client.data import Data
from spider.client.job import Job
from spider.client.taskgraph import TaskGraph
from spider.storage import MariaDBStorage, parse_jdbc_url
from spider.type import to_tdl_type_str


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
        self, jobs: Sequence[TaskGraph], args: Sequence[Sequence[object]]
    ) -> Sequence[Job]:
        """
        Submits a list of jobs to the storage.
        :param jobs: The list of task graphs to submit.
        :param args: The arguments for each job.
        :return: A sequence of Job objects representing the submitted jobs.
        :raises StorageError: If the jobs cannot be submitted to the storage.
        :raises ValueError: If the number of job inputs does not match the number of arguments.
        :raises TypeError: If the arguments are not of the expected type.
        :raises MsgpackError: If the arguments cannot be serialized with msgpack.
        """
        msg = "Number of job inputs does not match number of arguments"
        if len(jobs) != len(args):
            raise ValueError(msg)

        if not jobs:
            return []
        for task_graph, task_args in zip(jobs, args, strict=False):
            arg_index = 0
            for task_id in task_graph._impl.input_tasks:
                task = task_graph._impl.tasks[task_id]
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

        job_ids = self.storage.submit_jobs(self.driver_id, [job._impl for job in jobs])
        return [Job(job_id, self.storage) for job_id in job_ids]
