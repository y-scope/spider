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
        Creates a data in the storage.
        :param data:
        """
        self._storage.create_data_with_task_ref(self._task_id, data._impl)
