"""Executes a Spider Python task."""

from __future__ import annotations

import argparse
import inspect
import logging
from collections.abc import Sequence
from os import fdopen, getenv
from pydoc import locate
from types import FunctionType, GenericAlias
from typing import get_args, get_origin, get_type_hints, TYPE_CHECKING
from uuid import UUID

import msgpack

from spider_py import client
from spider_py.storage import MariaDBStorage, parse_jdbc_url, Storage
from spider_py.task_executor.task_executor_message import get_request_body, TaskExecutorResponseType
from spider_py.utils import from_serializable, to_serializable

if TYPE_CHECKING:
    from io import BufferedReader

# Set up logger
logger = logging.getLogger(__name__)


HeaderSize = 16


def parse_args() -> argparse.Namespace:
    """
    Parses task executor arguments.
    :return: The parsed arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--func", type=str, required=True, help="Name of the function to execute.")
    parser.add_argument(
        "--storage_url", type=str, required=False, help="JDBC URL for the storage backend."
    )
    parser.add_argument("--task_id", type=str, required=True, help="Task UUID.")
    parser.add_argument(
        "--input-pipe", type=int, required=True, help="File descriptor for the input pipe."
    )
    parser.add_argument(
        "--output-pipe", type=int, required=True, help="File descriptor for the output pipe."
    )
    return parser.parse_args()


def receive_message(pipe: BufferedReader) -> bytes:
    """
    Receives message from the pipe with a size header.
    :param pipe: Pipe to receive message from.
    :return: Received message body.
    :raises EOFError: If the message body size does not match header size.
    """
    body_size_str = pipe.read(HeaderSize).decode()
    body_size = int(body_size_str, base=10)
    body = pipe.read(body_size)
    if len(body) != body_size:
        msg = "Received message body size does not match the header size."
        raise EOFError(msg)
    return body


def parse_task_arguments(
    storage: Storage, params: list[inspect.Parameter], arguments: list[object]
) -> list[object]:
    """
    Parses arguments for the function to be executed.

    NOTE: `params` does not include the `TaskContext` parameter, and must be the same length as
    `arguments`. The caller is responsible for the size check.
    :param storage: Storage instance to use to get Data.
    :param params: A list of parameters in the function signature.
    :param arguments: A list of arguments to parse.
    :return: The parsed arguments.
    :raises TypeError: If a parameter has no type annotation or if an argument cannot be parsed.
    """
    parsed_args: list[object] = []
    for i, param in enumerate(params):
        arg = arguments[i]
        cls = param.annotation
        if param.annotation is inspect.Parameter.empty:
            msg = f"Parameter `{param.name}` has no type annotation."
            raise TypeError(msg)
        if cls is not client.Data:
            parsed_args.append(from_serializable(cls, arg))
            continue
        if not isinstance(arg, bytes):
            msg = f"Argument {i}: Expected `spider.Data` (bytes), but got {type(arg).__name__}."
            raise TypeError(msg)
        core_data = storage.get_data(UUID(bytes=arg))
        parsed_args.append(client.Data(core_data))
    return parsed_args


def parse_single_output_to_serializable(output: object, cls: type | GenericAlias) -> object:
    """
    Parses a single output from the function execution to a serializable form.
    :param output: Output to parse.
    :param cls: Expected output type.
    :return: The parsed output.
    """
    if isinstance(output, client.Data):
        return output.id.bytes
    return to_serializable(output, cls)


def parse_task_execution_results(
    results: object, types: type | GenericAlias | Sequence[type | GenericAlias]
) -> list[object]:
    """
    Parses results from the function execution.
    :param results: Results to parse.
    :param types: Expected output types. Must be a single type for non-tuple results, or a sequence
        of types matching the length of tuple results.
    :return: The parsed results.
    :raises TypeError: If the number of output types does not match the number of results.
    """
    response_messages: list[object] = [TaskExecutorResponseType.Result]
    if not isinstance(results, tuple):
        if not isinstance(types, (type, GenericAlias)):
            msg = "Invalid single output type."
            raise TypeError(msg)
        response_messages.append(parse_single_output_to_serializable(results, types))
        return response_messages
    # Parse as a tuple
    if not isinstance(types, Sequence) or len(results) != len(types):
        msg = "The number of output types does not match the number of results."
        raise TypeError(msg)
    for result, ret_type in zip(results, types, strict=True):
        response_messages.append(parse_single_output_to_serializable(result, ret_type))
    return response_messages


def get_return_types(
    func: FunctionType,
) -> type | GenericAlias | Sequence[type | GenericAlias]:
    """
    Gets the return types of a function.
    :param func: Function to get return types from.
    :return: Return types of the function. If the function returns a single value, the return type
        is a type or a generic alias. If the function returns multiple values, the return type is a
        sequence of types or generic aliases.
    :raises TypeError: If the function doesn't have return type annotation, or if the return type
        annotation is neither a type nor a generic alias.
    """
    signature = inspect.signature(func)
    annotation = signature.return_annotation

    if annotation is inspect.Signature.empty:
        msg = f"Function {func.__name__} has no return type annotation."
        raise TypeError(msg)

    # Resolve forward-referenced type annotations
    if isinstance(annotation, str):
        try:
            hints = get_type_hints(func)
            annotation = hints.get("return", annotation)
        except Exception as e:
            msg = f"Failed to get type hints for function {func.__name__}."
            raise TypeError(msg) from e

    origin = get_origin(annotation)
    if origin is not tuple:
        if not isinstance(annotation, (type, GenericAlias)):
            msg = (
                "Function return type annotation is neither a type nor a generic alias:"
                f" {annotation}."
            )
            raise TypeError(msg)
        return annotation
    return get_args(annotation)


def main() -> None:
    """Main function to execute the task."""
    # Parses arguments
    args = parse_args()
    function_name = args.func
    task_id = args.task_id
    task_id = UUID(task_id)
    input_pipe_fd = args.input_pipe
    output_pipe_fd = args.output_pipe

    storage_url_env = getenv("SPIDER_STORAGE_URL")
    if storage_url_env is not None:
        storage_url = storage_url_env
    elif args.storage_url is not None:
        logger.warning(
            "Prefer using `SPIDER_STORAGE_URL` environment variable over `--storage_url` argument."
        )
        storage_url = args.storage_url
    else:
        msg = (
            "Storage URL must be provided via `SPIDER_STORAGE_URL` environment variable or"
            " `--storage_url` argument."
        )
        raise ValueError(msg)

    logger.debug("Function to run: %s", function_name)

    # Sets up storage
    storage_params = parse_jdbc_url(storage_url)
    storage = MariaDBStorage(storage_params)

    with fdopen(input_pipe_fd, "rb") as input_pipe, fdopen(output_pipe_fd, "wb") as output_pipe:
        input_message = receive_message(input_pipe)
        arguments = get_request_body(input_message)
        logger.debug("Args buffer parsed")

        # Get the function to run
        function = locate(function_name)
        if function is None or not inspect.isfunction(function):
            msg = f"{function_name} cannot be found in the current Python execution environment."
            raise ValueError(msg)

        signature = inspect.signature(function)
        if len(signature.parameters) != len(arguments) + 1:
            msg = (
                f"Function {function_name} expects {len(signature.parameters) - 1} arguments, but"
                f" {len(arguments)} were provided."
            )
            raise ValueError(msg)
        task_context = client.TaskContext(task_id, storage)
        arguments = [
            task_context,
            *parse_task_arguments(storage, list(signature.parameters.values())[1:], arguments),
        ]
        try:
            results = function(*arguments)
            logger.debug("Function %s executed", function_name)
            return_types = get_return_types(function)
            responses = parse_task_execution_results(results, return_types)
        except Exception as e:
            logger.exception("Function %s failed", function_name)
            responses = [
                TaskExecutorResponseType.Error,
                {"type": e.__class__.__name__, "message": str(e)},
            ]

        packed_responses = msgpack.packb(responses)
        output_pipe.write(f"{len(packed_responses):0{HeaderSize}d}".encode())
        output_pipe.write(packed_responses)
        output_pipe.flush()


if __name__ == "__main__":
    main()
