"""Task module for Spider."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from uuid import UUID, uuid4

from spider_py.core.data import Data, DataId

TaskId = UUID


@dataclass
class TaskInputOutput:
    """Represents a task input that references the output of another task by its ID and position."""

    task_id: TaskId
    position: int


TaskInputValue = bytes
TaskInputData = Data | DataId


@dataclass
class TaskInput:
    """Represents a task input"""

    type: str
    value: TaskInputData | TaskInputOutput | TaskInputValue | None


TaskOutputValue = bytes
TaskOutputData = Data | DataId


@dataclass
class TaskOutput:
    """Represents a task output"""

    type: str
    value: TaskOutputData | TaskOutputValue


class TaskState(IntEnum):
    """Represents the state of a task"""

    Pending = 0
    Ready = 1
    Running = 2
    Succeeded = 3
    Failed = 4
    Cancelled = 5

    def __str__(self) -> str:
        """
        Returns string representation of task state.
        :return: The string representation of task state.
        """
        return _StateToStrMap[self]

    @staticmethod
    def from_str(state_str: str) -> TaskState:
        """
        Returns task state from string representation.
        :param state_str: The string representation of task state.
        :return: The task state from string representation.
        :raises ValueError: If the state string is not recognized.
        """
        state = _StrToStateMap.get(state_str)
        if state is not None:
            return state
        msg = f"Invalid task state string: {state_str}"
        raise ValueError(msg)


_StateToStrMap = {
    TaskState.Pending: "pending",
    TaskState.Ready: "ready",
    TaskState.Running: "running",
    TaskState.Succeeded: "success",
    TaskState.Failed: "fail",
    TaskState.Cancelled: "cancel",
}

_StrToStateMap = {value: key for key, value in _StateToStrMap.items()}


@dataclass
class Task:
    """Represents a task in Spider."""

    task_id: TaskId = field(default_factory=uuid4)
    function_name: str = ""
    state: TaskState = TaskState.Pending
    timeout: float = 0
    max_retries: int = 0
    task_inputs: list[TaskInput] = field(default_factory=list)
    task_outputs: list[TaskOutput] = field(default_factory=list)

    def set_pending(self) -> None:
        """Sets the task state to Pending."""
        self.state = TaskState.Pending

    def set_ready(self) -> None:
        """Sets the task state to Ready."""
        self.state = TaskState.Ready
