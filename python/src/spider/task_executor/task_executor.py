"""Executes a Spider Python task."""

import argparse
import importlib
import inspect
import logging
from io import BufferedReader
from os import fdopen
from types import GenericAlias
from uuid import UUID

from spider import client, core, storage
from spider.task_executor.task_executor_message import get_request_body


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

def parse_arguments(store: storage.Storage, params: list[inspect.Parameter], arguments: list[object]) -> list[object]:
    """
    Parses arguments for the function to be executed.
    :param store: Storage instance to use to get Data.
    :param params: List of parameters in the function signature.
    :param arguments: List of arguments to parse.
    :return: Parsed arguments.
    :raises TypeError: If a parameter has no type annotation or if an argument cannot be parsed.
    """
    parsed_args = []
    for i, param in enumerate(params):
        arg = arguments[i]
        cls = param.annotation
        if param.annotation is inspect.Parameter.empty:
            msg = f"Parameter {param.name} has no type annotation."
            raise TypeError(msg)
        if cls is bool:
            parsed_args.append(arg)
        elif cls is client.Data:
            core_data = store.get_data(UUID(arg))
            parsed_args.append(client.Data._from_impl(core_data))
        else:
            if isinstance(arg, list) or isinstance(arg, GenericAlias):
                parsed_args.append(cls(*arg))
            else:
                parsed_args.append(cls(arg))
    return parsed_args

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
    storage_url = args.storage_url
    input_pipe = args.input_pipe
    output_pipe = args.output_pipe

    logger.debug("Function to run: %s", func)

    # Sets up storage
    store = storage.MariaDBStorage(storage_url)

    input_pipe = fdopen(input_pipe, "rb")
    output_pipe = fdopen(output_pipe, "wb")
    input_message = receive_message(input_pipe)
    arguments = get_request_body(input_message)
    logger.debug("Args buffer parsed")

    # Get the function to run
    module_name, function_name = func.rsplit(".", 1)
    module = importlib.import_module(module_name)
    function = getattr(module, function_name)
    logger.debug("Function %s imported from module %s", function_name, module_name)

    signature = inspect.signature(function)
    if len(signature.parameters) != len(arguments) + 1:
        msg = (
            f"Function {function_name} expects {len(signature.parameters) - 1} "
            f"arguments, but {len(arguments)} were provided."
        )
        raise ValueError(msg)
    task_context = client.TaskContext(task_id, store)
    args = [task_context]
    for i, arg in enumerate(arguments):
        param = list(signature.parameters.values())[i]
        if param.annotation == inspect.Parameter.empty:
            raise
    results = function(*args)
    logger.debug("Function %s executed", function_name)


if __name__ == "__main__":
    main()
