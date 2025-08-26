"""TaskGraph module for Spider."""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from spider_py.core.task import Task

JobId = UUID


class TaskGraph:
    """
    Represents a task graph in Spider.
    TaskGraph represents a directed acyclic graph (DAG) of tasks.
    It stores:
    - tasks: A list of Task objects.
    - dependencies: A list of tuples representing the dependencies between tasks. Each tuple
      contains:
        - parent task index
        - child task index
    - input_tasks: A list of task indices that have no parents (input tasks).
    - output_tasks: A list of task indices that have no children (output tasks).
    - task_input_output_refs: A list of tuples representing the task inputs referencing task
      outputs of parent tasks. Each tuple contains:
      - input task index
      - input task's task input index
      - output task index
      - output task's task output index
    """

    def __init__(self) -> None:
        """Initializes an empty task graph."""
        self.tasks: list[Task] = []
        self.dependencies: list[tuple[int, int]] = []
        self.input_tasks: list[int] = []
        self.output_tasks: list[int] = []
        self.task_input_output_refs: list[tuple[int, int, int, int]] = []

    def add_task(self, task: Task) -> None:
        """
        Adds a task to the graph.
        :param task: The task to add.
        """
        self.tasks.append(task)
        index = len(self.tasks) - 1
        self.input_tasks.append(index)
        self.output_tasks.append(index)

    def merge_graph(self, graph: TaskGraph) -> None:
        """
        Merges another task graph into this task graph.
        :param graph: The task graph to merge.
        """
        index_offset = len(self.tasks)
        self.tasks.extend(graph.tasks)
        self.dependencies.extend(
            [
                (parent + index_offset, child + index_offset)
                for (parent, child) in graph.dependencies
            ]
        )
        self.input_tasks.extend([index + index_offset for index in graph.input_tasks])
        self.output_tasks.extend([index + index_offset for index in graph.output_tasks])
        self.task_input_output_refs.extend(
            [
                (
                    input_index + index_offset,
                    input_position,
                    output_index + index_offset,
                    output_position,
                )
                for (
                    input_index,
                    input_position,
                    output_index,
                    output_position,
                ) in graph.task_input_output_refs
            ]
        )

    @staticmethod
    def chain_graph(parent: TaskGraph, child: TaskGraph) -> TaskGraph:
        """
        Chains two task graphs into a new task graph.
        :param parent: The parent task graph.
        :param child: The child task graph.
        :return: The chained task graph.
        :raise TypeError: If the outputs and the inputs of `graph` do not match.
        """
        graph = deepcopy(parent)
        index_offset = len(graph.tasks)
        parent_output_tasks = graph.output_tasks
        graph.tasks.extend(child.tasks)
        graph.dependencies.extend(
            [
                (parent_index + index_offset, child_index + index_offset)
                for (parent_index, child_index) in child.dependencies
            ]
        )
        graph.output_tasks = [index + index_offset for index in child.output_tasks]

        size_mismatch_msg = "Parent outputs size and child inputs size do not match."

        task_output_index, output_position = 0, 0
        for index in child.input_tasks:
            input_task_index = index + index_offset
            input_task = graph.tasks[input_task_index]
            for i in range(len(input_task.task_inputs)):
                if task_output_index >= len(parent_output_tasks):
                    raise TypeError(size_mismatch_msg)
                output_task_index = parent_output_tasks[task_output_index]

                if (output_task_index, input_task_index) not in graph.dependencies:
                    graph.dependencies.append((output_task_index, input_task_index))

                input_type = input_task.task_inputs[i].type
                output_type = graph.tasks[output_task_index].task_outputs[output_position].type
                if input_type != output_type:
                    msg = f"Output type {output_type} does not match input type {input_type}"
                    raise TypeError(msg)
                graph.task_input_output_refs.append(
                    (input_task_index, i, output_task_index, output_position)
                )
                output_position += 1
                if output_position >= len(graph.tasks[output_task_index].task_outputs):
                    output_position = 0
                    task_output_index += 1

        if task_output_index != len(parent_output_tasks) or output_position != 0:
            raise TypeError(size_mismatch_msg)

        graph.task_input_output_refs.extend(
            [
                (
                    input_index + index_offset,
                    input_position,
                    output_index + index_offset,
                    output_position,
                )
                for (
                    input_index,
                    input_position,
                    output_index,
                    output_position,
                ) in child.task_input_output_refs
            ]
        )

        return graph
