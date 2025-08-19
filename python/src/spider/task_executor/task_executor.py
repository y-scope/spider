"""Executes a Spider Python task."""

import argparse
import importlib
import inspect
import logging
from io import BufferedReader
from os import fdopen
from uuid import UUID

from spider.storage import MariaDBStorage
from spider.task_executor.task_executor_message import get_request_body

from spider.client import TaskContext

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
    storage = MariaDBStorage(storage_url)

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
    task_context = TaskContext(task_id, storage)


if __name__ == "__main__":
    main()
