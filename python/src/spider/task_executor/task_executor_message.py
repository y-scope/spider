"""Task executor message module."""

from enum import IntEnum


class TaskExecutorResponseType(IntEnum):
    """Task executor response type."""

    Unknown = 0
    Result = 1
    Error = 2
    Block = 3
    Ready = 4
    Cancel = 5

class TaskExecutorRequestType(IntEnum):
    """Task executor request type."""

    Unknown = 0
    Arguments = 1
    Resume = 2

