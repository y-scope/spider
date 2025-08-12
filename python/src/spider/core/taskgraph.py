"""TaskGraph module for Spider."""

from copy import deepcopy
from uuid import uuid4

from spider.core.task import Task, TaskId, TaskInputOutput


class TaskGraph:
    """Represents a task graph in Spider."""

    def __init__(self) -> None:
        """Initializes an empty task graph."""
        self.tasks: dict[TaskId, Task] = {}
        # Dependency list consists of a list of tuples of
        #   - parent task id
        #   - child task id
        self.dependencies: list[tuple[TaskId, TaskId]] = []
        self.input_tasks: list[TaskId] = []
        self.output_tasks: list[TaskId] = []

    def add_task(
        self, task: Task, parents: list[TaskId] | None = None, children: list[TaskId] | None = None
    ) -> None:
        """
        Adds a task to the graph.
        :param task: The task to add.
        :param parents: The parent ids of the task. Must be already in the task graph.
        :param children: The children ids of the task. Must be already in the task graph.
        """
        self.tasks[task.task_id] = deepcopy(task)
        if parents is not None and len(parents) > 0:
            for parent in parents:
                self.dependencies.append((parent, task.task_id))
                if parent in self.output_tasks:
                    self.output_tasks.remove(parent)
        else:
            self.input_tasks.append(task.task_id)
        if children is not None and len(children) > 0:
            for child in children:
                self.dependencies.append((task.task_id, child))
                if child in self.output_tasks:
                    self.input_tasks.remove(child)
        else:
            self.output_tasks.append(task.task_id)

    def get_parents(self, task_id: TaskId) -> list[Task]:
        """
        Gets parent tasks of task.
        :param task_id: ID of the task.
        :return: List of parent tasks.
        """
        return [self.tasks[parent] for (parent, child) in self.dependencies if child == task_id]

    def get_children(self, task_id: TaskId) -> list[Task]:
        """
        Gets child tasks of task.
        :param task_id: ID of the task.
        :return: List of children tasks.
        """
        return [self.tasks[child] for (parent, child) in self.dependencies if parent == task_id]

    def reset_ids(self) -> None:
        """Resets task ids."""
        id_map = {}
        for task_id in self.tasks:
            id_map[task_id] = uuid4()

        new_tasks = {}
        for task_id in self.tasks:
            new_task_id = id_map[task_id]
            new_tasks[new_task_id] = deepcopy(self.tasks[task_id])
            new_tasks[new_task_id].task_id = new_task_id
            for task_input in new_tasks[new_task_id].task_inputs:
                if isinstance(task_input.value, TaskInputOutput):
                    task_input.value.task_id = id_map[task_input.value.task_id]
        self.tasks = new_tasks

        new_dependencies = []
        for parent, child in self.dependencies:
            new_dependencies.append((id_map[parent], id_map[child]))
        self.dependencies = new_dependencies

        new_input_tasks = []
        for task_id in self.input_tasks:
            new_input_tasks.append(id_map[task_id])
        self.input_tasks = new_input_tasks

        new_output_tasks = []
        for task_id in self.output_tasks:
            new_output_tasks.append(id_map[task_id])
        self.output_tasks = new_output_tasks

    def merge_graph(self, graph: "TaskGraph") -> None:
        """
        Merges another task graph into this task graph.
        :param graph: The task graph to merge.
        :return:
        """
        new_graph = deepcopy(graph)
        new_graph.reset_ids()
        self.tasks.update(new_graph.tasks)
        self.dependencies.extend(new_graph.dependencies)
        self.input_tasks.extend(new_graph.input_tasks)
        self.output_tasks.extend(new_graph.output_tasks)

    def chain_graph(self, child: "TaskGraph") -> "TaskGraph":
        """
        Chains another task graph with this task graph.
        :param child: The task graph to be chained as child.
        :return: The chained task graph.
        :raise TypeError: If the outputs and the inputs of `graph` do not match.
        """
        graph = deepcopy(self)
        graph.reset_ids()
        parent_output_tasks = graph.output_tasks
        graph.tasks.update(child.tasks)
        graph.dependencies.extend(child.dependencies)
        graph.output_tasks = deepcopy(child.output_tasks)

        size_mismatch_msg = "Parent outputs size and child inputs size do not match."

        task_index, output_position = 0, 0
        for task_id in child.input_tasks:
            input_task = graph.tasks[task_id]
            for i in range(len(input_task.task_inputs)):
                if task_index >= len(parent_output_tasks):
                    raise TypeError(size_mismatch_msg)
                output_task_id = parent_output_tasks[task_index]

                if (output_task_id, task_id) not in graph.dependencies:
                    graph.dependencies.append((output_task_id, task_id))

                input_type = input_task.task_inputs[i].type
                output_type = graph.tasks[output_task_id].task_outputs[output_position].type
                if input_type != output_type:
                    msg = f"Output type {output_type} does not match input type {input_type}"
                    raise TypeError(msg)
                input_task.task_inputs[i].value = TaskInputOutput(output_task_id, output_position)
                output_position += 1
                if output_position >= len(graph.tasks[output_task_id].task_outputs):
                    output_position = 0
                    task_index += 1

        if task_index != len(parent_output_tasks) or output_position != 0:
            raise TypeError(size_mismatch_msg)

        graph.reset_ids()
        return graph
