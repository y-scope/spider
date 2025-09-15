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
    :raises msgpack.exceptions.UnpackException: If the data is not a valid msgpack serialized list.
    :raises TypeError: If the data is not a msgpack list or the list is too short.
    :raises InvalidRequestTypeError: If the message header is not an `Arguments`.
    """
    data = msgpack.unpackb(message)
    if not isinstance(data, list):
        msg = "Message is not a list."
        raise TypeError(msg)
    if len(data) != ArgRequestLength:
        msg = "Message is too short."
        raise TypeError(msg)
    message_header = int(data[0])
    if TaskExecutorRequestType.Arguments != message_header:
        msg = f"Message header is not an `Arguments`: {message_header}"
        raise InvalidRequestTypeError(msg)
    return cast("list[object]", data[1])
