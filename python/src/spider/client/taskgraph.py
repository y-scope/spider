"""Spider client TaskGraph module."""

from spider import core
from spider.client.task import create_task, TaskFunction


class TaskGraph:
    """
    Spider client TaskGraph class.
    Warps around the core TaskGraph class.
    """

    def __init__(self) -> None:
        """Initialize TaskGraph."""
        self._impl = core.TaskGraph()


def group(tasks: list[TaskFunction | TaskGraph]) -> TaskGraph:
    """
    Groups task functions and task graph into a single task graph.
    :param tasks: List of task functions or task graphs.
    :return: The new task graph.
    """
    graph = TaskGraph()
    for task in tasks:
        if isinstance(task, TaskFunction):
            graph._impl.add_task(create_task(task))
        else:
            graph._impl.merge_graph(task._impl)
            graph._impl.reset_ids()

    return graph
