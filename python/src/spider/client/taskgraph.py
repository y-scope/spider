"""Spider client TaskGraph module."""

from collections.abc import Sequence

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

    def chain_graph(self, child: "TaskGraph") -> "TaskGraph":
        """
        Chains another task graph with this task graph.
        :param child: The task graph to be chained as child.
        :return: The chained task graph.
        :raise TypeError: If the outputs and the inputs of `graph` do not match.
        """
        graph = TaskGraph()
        graph._impl = self._impl.chain_graph(child._impl)
        return graph


def group(tasks: Sequence[TaskFunction | TaskGraph]) -> TaskGraph:
    """
    Groups task functions and task graph into a single task graph.
    :param tasks: List of task functions or task graphs.
    :return: The new task graph.
    """
    graph = TaskGraph()
    for task in tasks:
        if callable(task):
            graph._impl.add_task(create_task(task))
        else:
            graph._impl.merge_graph(task._impl)

    return graph


def chain(parent: TaskFunction | TaskGraph, child: TaskFunction | TaskGraph) -> TaskGraph:
    """
    Chains two task functions or task graphs into a single task graph.
    :param parent:
    :param child:
    :return:
    :raises TypeError: If the parent outputs and child inputs do not match.
    """
    if callable(parent):
        task = create_task(parent)
        parent = TaskGraph()
        parent._impl.add_task(task)
    if callable(child):
        task = create_task(child)
        child = TaskGraph()
        child._impl.add_task(task)
    return parent.chain_graph(child)
