"""Spider client task context module."""

from spider_py import core
from spider_py.client.data import Data
from spider_py.storage import Storage


class TaskContext:
    """
    Represents the task context, providing:
    - Access to the task ID.
    - Task-referenced Data creation.
    """

    def __init__(self, task_id: core.TaskId, storage: Storage) -> None:
        """
        Initializes the task context.
        :param task_id:
        :param storage:
        """
        self._task_id = task_id
        self._storage = storage

    @property
    def task_id(self) -> core.TaskId:
        """:return: The task id."""
        return self._task_id

    def create_data(self, data: Data) -> None:
        """
        Creates a task-ID referenced data object in the storage.
        :param data:
        """
        self._storage.create_data_with_task_ref(self._task_id, data._impl)
