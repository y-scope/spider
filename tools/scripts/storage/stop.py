#!/usr/bin/env -S uv run --script
# /// script
# dependencies = []
# ///
"""Script to stop a running MariaDB Docker container."""

import argparse
import logging
import subprocess
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> int:
    """Main."""
    # To silence Ruff S607
    docker_executable = "docker"

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--name",
        type=str,
        default="mariadb-spider-dev",
        help="The name of the started MariaDB container (default: %(default)s)",
    )
    args = parser.parse_args()

    result = subprocess.run(
        [docker_executable, "inspect", "-f", "{{.State.Running}}", args.name],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or result.stdout.rstrip("\n") != "true":
        logger.warning("Container '%s' doesn't exist. Exit peacefully.", args.name)
        return 0

    localstack_stop_cmd = [
        "docker",
        "stop",
        args.name,
    ]

    result = subprocess.run(localstack_stop_cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        logger.error("Failed to stop MariaDB container:\n%s", result.stderr)
        return result.returncode
    logger.info("MariaDB container stopped successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
