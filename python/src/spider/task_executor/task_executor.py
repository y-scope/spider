"""Executes a Spider Python task."""

import argparse
import logging
from uuid import UUID

from src.spider.storage import MariaDBStorage

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

    logger.debug(f"Function to run: {func}")

    # Sets up storage
    storage = MariaDBStorage(storage_url)


if __name__ == "__main__":
    main()
