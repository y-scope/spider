"""Executes a Spider Python task."""

import argparse
import importlib
import inspect
import logging
from collections.abc import Callable
from io import BufferedReader
from os import fdopen
from types import ModuleType
from uuid import UUID

import msgpack

from spider_py import client
from spider_py.storage import MariaDBStorage, parse_jdbc_url, Storage
from spider_py.task_executor.task_executor_message import get_request_body, TaskExecutorResponseType
from spider_py.utils import from_serializable, to_serializable

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
        "--libs", nargs="+", type=str, required=True, help="List of libraries to load."
    )
    parser.add_argument(
        "--storage_url", type=str, required=True, help="JDBC URL for the storage backend."
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
    :param storage: Storage instance to use to get Data.
    :param params: List of parameters in the function signature.
    :param arguments: List of arguments to parse. Must be the same length as params.
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


def parse_single_output_to_serializable(output: object) -> object:
    """
    Parses a single output from the function execution to a serializable form.
    :param output: Output to parse.
    :return: The parsed output.
    """
    if isinstance(output, client.Data):
        return output.id.bytes
    return to_serializable(output)


def parse_task_execution_results(results: object) -> list[object]:
    """
    Parses results from the function execution.
    :param results: Results to parse.
    :return: The parsed results.
    """
    response_messages: list[object] = [TaskExecutorResponseType.Result]
    if not isinstance(results, tuple):
        response_messages.append(parse_single_output_to_serializable(results))
        return response_messages
    # Parse as a tuple
    for result in results:
        response_messages.append(parse_single_output_to_serializable(result))
    return response_messages


def get_function_from_module(
    module: ModuleType, function_name: str
) -> Callable[..., object] | None:
    """
    Gets a function from a module by name.
    :param module:
    :param function_name: qualname of the function.
    :return: The function found.
    :return: None if the `function_name` does not exist in `module`.
    :return: None if the attribute matching the `function_name` is not a function.
    """
    obj = module
    for attr in function_name.split("."):
        if not hasattr(obj, attr):
            return None
        obj = getattr(obj, attr)
    if not inspect.isfunction(obj):
        return None
    return obj


def main() -> None:
    """Main function to execute the task."""
    # Parses arguments
    args = parse_args()
    function_name = args.func
    task_id = args.task_id
    task_id = UUID(task_id)
    mods = args.libs
    storage_url = args.storage_url
    input_pipe_fd = args.input_pipe
    output_pipe_fd = args.output_pipe

    logger.debug("Function to run: %s", function_name)

    # Sets up storage
    storage_params = parse_jdbc_url(storage_url)
    storage = MariaDBStorage(storage_params)

    with fdopen(input_pipe_fd, "rb") as input_pipe, fdopen(output_pipe_fd, "wb") as output_pipe:
        input_message = receive_message(input_pipe)
        arguments = get_request_body(input_message)
        logger.debug("Args buffer parsed")

        # Get the function to run
        function = None
        for mod in mods:
            module = importlib.import_module(mod)
            function = get_function_from_module(module, function_name)
            if function is not None:
                break
        if function is None:
            msg = f"Function {function_name} not found in provided libraries."
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
            responses = parse_task_execution_results(results)
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
