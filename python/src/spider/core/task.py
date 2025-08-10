"""Task module for Spider."""

from dataclasses import dataclass, field
from enum import IntEnum
from uuid import UUID

from spider.core.data import DataId

TaskId = UUID


@dataclass
class TaskInputOutput:
    """Represents a task input that points to output of another task"""

    task_id: TaskId
    position: int


TaskInputValue = bytes
TaskInputData = DataId
TaskInput = TaskInputOutput | TaskInputValue | TaskInputData

TaskOutputValue = bytes
TaskOutputData = DataId
TaskOutput = TaskOutputValue | TaskOutputData


class TaskState(IntEnum):
    """Represents state of a task"""

    Pending = 0
    Ready = 1
    Running = 2
    Succeeded = 3
    Failed = 4
    Cancelled = 5


@dataclass
class Task:
    """Represents a task in Spider."""

    task_id: TaskId
    function_name: str
    state: TaskState
    timeout: float
    max_retries: int
    task_inputs: list[TaskInput] = field(default_factory=list)
    task_outputs: list[TaskOutput] = field(default_factory=list)
