"""Spider client TaskGraph module."""

from __future__ import annotations

from typing import TYPE_CHECKING

from spider_py import core
from spider_py.client.task import create_task, TaskFunction

if TYPE_CHECKING:
    from collections.abc import Sequence


class TaskGraph:
    """
    Represents a client-side task graph.

    This class is a wrapper of `spider_py.core.Task`.
    """

    def __init__(self) -> None:
        """Initializes TaskGraph."""
        self._impl = core.TaskGraph()


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
    :return: The new task graph.
    :raises TypeError: If the parent outputs and child inputs do not match.
    """
    parent_core_graph: core.TaskGraph
    child_core_graph: core.TaskGraph

    if callable(parent):
        parent_core_graph = core.TaskGraph()
        parent_core_graph.add_task(create_task(parent))
    else:
        parent_core_graph = parent._impl

    if callable(child):
        child_core_graph = core.TaskGraph()
        child_core_graph.add_task(create_task(child))
    else:
        child_core_graph = child._impl

    graph = TaskGraph()
    graph._impl = core.TaskGraph.chain_graph(parent_core_graph, child_core_graph)
    return graph
