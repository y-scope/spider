"""TaskGraph module for Spider."""

from spider_py.core.task import Task, TaskId


class TaskGraph:
    """
    Represents a task graph in Spider.
    TaskGraph represents a directed acyclic graph (DAG) of tasks.
    It stores:
    - tasks: A dictionary mapping task ids to Task objects.
    - dependencies: A list of tuples representing the dependencies between tasks. Each tuple
      contains:
        - parent task id
        - child task id
    - input_tasks: A set of task ids that have no parents (input tasks).
    - output_tasks: A set of task ids that have no children (output tasks).
    """

    def __init__(self) -> None:
        """Initializes an empty task graph."""
        self.tasks: dict[TaskId, Task] = {}
        self.dependencies: list[tuple[TaskId, TaskId]] = []
        self.input_tasks: set[TaskId] = set()
        self.output_tasks: set[TaskId] = set()

    def add_task(
        self, task: Task, parents: list[TaskId] | None = None, children: list[TaskId] | None = None
    ) -> None:
        """
        Adds a task to the graph.
        :param task: The task to add.
        :param parents: The parent ids of the task. Must be already in the task graph.
        :param children: The children ids of the task. Must be already in the task graph.
        """
        self.tasks[task.task_id] = task
        if parents:
            for parent in parents:
                self.dependencies.append((parent, task.task_id))
                self.output_tasks.discard(parent)
        else:
            self.input_tasks.add(task.task_id)
        if children:
            for child in children:
                self.dependencies.append((task.task_id, child))
                self.input_tasks.discard(child)
        else:
            self.output_tasks.add(task.task_id)

    def get_parents(self, task_id: TaskId) -> list[Task]:
        """
        :param task_id:
        :return: Parent tasks of the task identified by `task_id`.
        """
        return [self.tasks[parent] for (parent, child) in self.dependencies if child == task_id]

    def get_children(self, task_id: TaskId) -> list[Task]:
        """
        :param task_id:
        :return: Child tasks of the task identified by `task_id`.
        """
        return [self.tasks[child] for (parent, child) in self.dependencies if parent == task_id]
