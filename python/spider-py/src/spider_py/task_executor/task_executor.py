"""Executes a Spider Python task."""

import argparse
import importlib
import inspect
import logging
from io import BufferedReader
from os import fdopen
from uuid import UUID

import msgpack

from spider_py import client, storage
from spider_py.task_executor.task_executor_message import get_request_body, TaskExecutorResponseType
from spider_py.utils import from_serializable, to_serializable

# Set up logger
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """
    Parses task executor arguments.
    :return: Parsed arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--func", type=str, required=True)
    parser.add_argument("--libs", nargs="+", type=str, required=True)
    parser.add_argument("--storage_url", type=str, required=True)
    parser.add_argument("--task_id", type=str, required=True)
    parser.add_argument("--input-pipe", type=int, required=True)
    parser.add_argument("--output-pipe", type=int, required=True)
    return parser.parse_args()


HeaderSize = 16


def receive_message(pipe: BufferedReader) -> bytes:
    """
    Receives message from the pipe with a size header.
    :param pipe: Pipe to receive message from.
    :return: Received message body.
    :raises IOError: If read from pipe fails.
    :raises UnicodeDecodeError: If parsing header fails.
    :raises EOFError: If the message body size does not match header size.
    """
    body_size_str = pipe.read(HeaderSize).decode()
    body_size = int(body_size_str, base=10)
    body = pipe.read(body_size)
    if len(body) != body_size:
        msg = "Received message body size does not match header size."
        raise EOFError(msg)
    return body


def parse_arguments(
    store: storage.Storage, params: list[inspect.Parameter], arguments: list[object]
) -> list[object]:
    """
    Parses arguments for the function to be executed.
    :param store: Storage instance to use to get Data.
    :param params: List of parameters in the function signature.
    :param arguments: List of arguments to parse.
    :return: Parsed arguments.
    :raises TypeError: If a parameter has no type annotation or if an argument cannot be parsed.
    """
    parsed_args: list[object] = []
    for i, param in enumerate(params):
        arg = arguments[i]
        cls = param.annotation
        if param.annotation is inspect.Parameter.empty:
            msg = f"Parameter {param.name} has no type annotation."
            raise TypeError(msg)
        if cls is client.Data:
            if not isinstance(arg, bytes):
                msg = f"Argument {i} for spider.Data is not bytes."
                raise TypeError(msg)
            core_data = store.get_data(UUID(bytes=arg))
            parsed_args.append(client.Data(core_data))
        else:
            parsed_args.append(from_serializable(cls, arg))
    return parsed_args


def parse_results(results: object) -> list[object]:
    """
    Parses results from the function execution.
    :param results: Results to parse.
    :return: Parsed results.
    :raises TypeError: If a result cannot be parsed.
    """
    response_messages: list[object] = [TaskExecutorResponseType.Result]
    if isinstance(results, tuple):
        for result in results:
            if isinstance(result, client.Data):
                response_messages.append(result.id.bytes)
            else:
                response_messages.append(to_serializable(result))
    elif isinstance(results, client.Data):
        response_messages.append(results.id.bytes)
    else:
        response_messages.append(to_serializable(results))
    return response_messages


def main() -> None:
    """
    Main function to execute the task.
    :raises StorageError: If a storage operation fails.
    """
    # Parses arguments
    args = parse_args()
    func = args.func
    task_id = args.task_id
    task_id = UUID(task_id)
    libs = args.libs
    storage_url = args.storage_url
    input_pipe_fd = args.input_pipe
    output_pipe_fd = args.output_pipe

    logger.debug("Function to run: %s", func)

    # Sets up storage
    storage_params = storage.parse_jdbc_url(storage_url)
    store = storage.MariaDBStorage(storage_params)

    with fdopen(input_pipe_fd, "rb") as input_pipe, fdopen(output_pipe_fd, "wb") as output_pipe:
        input_message = receive_message(input_pipe)
        arguments = get_request_body(input_message)
        logger.debug("Args buffer parsed")

        # Get the function to run
        function_name = func.replace(".")[-1] if "." in func else func
        function = None
        for lib in libs:
            module = importlib.import_module(lib)
            if hasattr(module, function_name):
                function = getattr(module, function_name)
                break
        if function is None:
            msg = f"Function {function_name} not found in provided libraries."
            raise ValueError(msg)

        signature = inspect.signature(function)
        if len(signature.parameters) != len(arguments) + 1:
            msg = (
                f"Function {function_name} expects {len(signature.parameters) - 1} "
                f"arguments, but {len(arguments)} were provided."
            )
            raise ValueError(msg)
        task_context = client.TaskContext(task_id, store)
        arguments = [
            task_context,
            *parse_arguments(store, list(signature.parameters.values())[1:], arguments),
        ]
        results = function(*arguments)
        logger.debug("Function %s executed", function_name)

        responses = parse_results(results)
        packed_responses = msgpack.packb(responses)
        output_pipe.write(f"{len(packed_responses):0{HeaderSize}d}".encode())
        output_pipe.write(packed_responses)
        output_pipe.flush()


if __name__ == "__main__":
    main()
