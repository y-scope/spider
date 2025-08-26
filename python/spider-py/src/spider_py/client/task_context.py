"""Spider client task context module."""

from spider_py import core, storage
from spider_py.client.data import Data


class TaskContext:
    """Spider task context provides access to the task id and data creation for tasks."""

    def __init__(self, task_id: core.TaskId, storage: storage.Storage) -> None:
        """Initializes the task context."""
        self._task_id = task_id
        self._storage = storage

    @property
    def task_id(self) -> core.TaskId:
        """:return: The task id."""
        return self._task_id

    def create_data(self, data: Data) -> None:
        """
        Creates a new data object in the storage associated with the task.
        :param data: The data object to be created.
        :raises StorageError: If there is an error during storage operation.
        """
        self._storage.create_task_data(self._task_id, data._impl)
