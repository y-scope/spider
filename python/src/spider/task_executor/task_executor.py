"""Executes a Spider Python task."""

import argparse
import logging
from io import BufferedReader
from os import fdopen
from uuid import UUID

from spider.storage import MariaDBStorage

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
    libs = args.libs
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
    input_data = receive_message(input_pipe)


if __name__ == "__main__":
    main()
