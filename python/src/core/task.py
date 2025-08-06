"""Task module for Spider."""

from dataclasses import dataclass, field
from enum import IntEnum
from uuid import UUID


@dataclass
class TaskInputOutput:
    """Represents a task input that points to output of another task"""

    task_id: UUID
    position: int


TaskInputValue = bytes
TaskInputData = UUID
TaskInput = TaskInputOutput | TaskInputValue | TaskInputData

TaskOutputValue = bytes
TaskOutputData = UUID
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

    task_id: UUID
    function_name: str
    state: TaskState
    timeout: float
    max_retires: int
    task_input: list[TaskInputData] = field(default_factory=list)
    task_output: list[TaskOutputData] = field(default_factory=list)
