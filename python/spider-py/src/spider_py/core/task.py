"""Task module for Spider."""

from dataclasses import dataclass, field
from enum import IntEnum
from uuid import UUID, uuid4

from spider_py.core.data import DataId

TaskId = UUID


@dataclass
class TaskInputOutput:
    """Represents a task input that points to output of another task"""

    task_id: TaskId
    position: int


TaskInputValue = bytes
TaskInputData = DataId


@dataclass
class TaskInput:
    """Represents a task input"""

    type: str
    value: TaskInputData | TaskInputOutput | None


TaskOutputValue = bytes
TaskOutputData = DataId


@dataclass
class TaskOutput:
    """Represents a task output"""

    type: str
    value: TaskOutputData | TaskOutputValue


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

    task_id: TaskId = field(default_factory=uuid4)
    function_name: str = ""
    state: TaskState = TaskState.Pending
    timeout: float = 0
    max_retries: int = 0
    task_inputs: list[TaskInput] = field(default_factory=list)
    task_outputs: list[TaskOutput] = field(default_factory=list)
