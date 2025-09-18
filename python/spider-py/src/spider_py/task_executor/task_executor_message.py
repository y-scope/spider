"""Task executor message module."""

from enum import IntEnum
from typing import cast

import msgpack


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


class InvalidRequestTypeError(Exception):
    """Exception raised for invalid request types."""

    def __init__(self, message: str) -> None:
        """Initializes the InvalidRequestTypeError with a message."""
        super().__init__(message)


ArgRequestLength = 2


def get_request_body(message: bytes) -> list[object]:
    """
    Gets the request body from the request message.
    :param message: The msgpack serialized request message.
    :return: The request body as a list of objects.
    :raises TypeError: If the serialized message is not a msgpack list or the list length is
        incorrect.
    :raises InvalidRequestTypeError: If the message header is not
        `TaskExecutorRequestType.Arguments`.
    """
    data = msgpack.unpackb(message)
    if not isinstance(data, list):
        msg = "Message is not a list."
        raise TypeError(msg)
    if len(data) != ArgRequestLength:
        msg = f"Message is not a list with {ArgRequestLength} elements, got {len(data)}."
        raise TypeError(msg)
    header = data[0]
    if not isinstance(header, int):
        msg = "Message header is not an int."
        raise TypeError(msg)
    if TaskExecutorRequestType.Arguments != header:
        msg = f"Message header is not an `Arguments`: {header}."
        raise InvalidRequestTypeError(msg)
    body = data[1]
    if not isinstance(body, list):
        msg = "Message body is not a list."
        raise TypeError(msg)
    return cast("list[object]", body)
