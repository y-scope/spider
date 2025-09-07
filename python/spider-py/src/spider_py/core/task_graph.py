"""TaskGraph module for Spider."""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spider_py.core.task import Task


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
    - input_task_indices: A list of task indices that have no parents (input tasks).
    - output_task_indices: A list of task indices that have no children (output tasks).
    - task_input_output_refs: A list of `InputOutputRef` representing references from task inputs
      to task outputs.
    """

    def __init__(self) -> None:
        """Initializes an empty task graph."""
        self.tasks: list[Task] = []
        self.dependencies: list[tuple[int, int]] = []
        self.input_task_indices: list[int] = []
        self.output_task_indices: list[int] = []
        self.task_input_output_refs: list[TaskGraph.InputOutputRef] = []

    class InputOutputRef:
        """Represents a reference from a task input to a task output."""

        def __init__(
            self,
            input_task_index: int,
            input_position: int,
            output_task_index: int,
            output_position: int,
        ) -> None:
            """Initializes an InputOutputRef object."""
            self.input_task_index: int = input_task_index
            self.input_position: int = input_position
            self.output_task_index: int = output_task_index
            self.output_position: int = output_position

        def with_offset(self, index_offset: int) -> TaskGraph.InputOutputRef:
            """
            Creates a new `InputOutputRef` with the task indices adjusted by applying the specified
            offset.

            :param index_offset: The offset to apply to the task indices.
            :return: A new `InputOutputRef` instance with adjusted task indices.
            """
            return TaskGraph.InputOutputRef(
                self.input_task_index + index_offset,
                self.input_position,
                self.output_task_index + index_offset,
                self.output_position,
            )

    def add_task(self, task: Task) -> None:
        """
        Adds a task to the graph.
        :param task: The task to add.
        """
        index = len(self.tasks)
        self.tasks.append(task)
        self.input_task_indices.append(index)
        self.output_task_indices.append(index)

    def copy(self) -> TaskGraph:
        """:return: A deep copy of the task graph."""
        return deepcopy(self)

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
        self.input_task_indices.extend([index + index_offset for index in graph.input_task_indices])
        self.output_task_indices.extend(
            [index + index_offset for index in graph.output_task_indices]
        )
        self.task_input_output_refs.extend(
            [ref.with_offset(index_offset) for ref in graph.task_input_output_refs]
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
        parent_output_task_indices = graph.output_task_indices
        graph.tasks.extend(child.tasks)
        graph.dependencies.extend(
            [
                (parent_index + index_offset, child_index + index_offset)
                for (parent_index, child_index) in child.dependencies
            ]
        )
        graph.output_task_indices = [index + index_offset for index in child.output_task_indices]

        size_mismatch_msg = "Parent outputs size and child inputs size do not match."

        parent_output_task_it = iter(parent_output_task_indices)
        output_task_index = next(parent_output_task_it, None)
        output_position = 0
        for input_task_index in (i + index_offset for i in child.input_task_indices):
            input_task = graph.tasks[input_task_index]
            for input_position, task_input in enumerate(input_task.task_inputs):
                if output_task_index is None:
                    raise TypeError(size_mismatch_msg)

                if (output_task_index, input_task_index) not in graph.dependencies:
                    graph.dependencies.append((output_task_index, input_task_index))

                task_outputs = graph.tasks[output_task_index].task_outputs
                if output_position >= len(task_outputs):
                    raise TypeError(size_mismatch_msg)
                output_type = task_outputs[output_position].type
                input_type = task_input.type
                if input_type != output_type:
                    msg = f"Output type {output_type} does not match input type {input_type}"
                    raise TypeError(msg)

                graph.task_input_output_refs.append(
                    TaskGraph.InputOutputRef(
                        input_task_index, input_position, output_task_index, output_position
                    )
                )

                output_position += 1
                if output_position >= len(task_outputs):
                    output_position = 0
                    output_task_index = next(parent_output_task_it, None)

        if output_task_index is not None or output_position != 0:
            raise TypeError(size_mismatch_msg)

        graph.task_input_output_refs.extend(
            [ref.with_offset(index_offset) for ref in child.task_input_output_refs]
        )

        return graph
