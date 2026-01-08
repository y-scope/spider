#!/usr/bin/env -S uv run --script
# /// script
# dependencies = []
# ///
"""Script to wait for MariaDB Docker container ready for connections."""

import argparse
import logging
import subprocess
import sys
import time

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

    parser = argparse.ArgumentParser(
        description="Wait for MariaDB Docker container ready for connections."
    )
    parser.add_argument(
        "--name",
        type=str,
        default="mariadb-spider-dev",
        help="The name of the started MariaDB Docker container (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="The timeout in seconds to wait for the container to be ready (default: %(default)s)",
    )
    args = parser.parse_args()

    start = time.time()
    while True:
        result = subprocess.run(
            [
                docker_executable,
                "exec",
                args.name,
                "healthcheck.sh",
                "--connect",
                "--innodb_initialized",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            logger.info("MariaDB container '%s' is ready for connections.", args.name)
            return 0
        if time.time() - start > args.timeout:
            logger.error("Timeout reached. MariaDB container '%s' is not ready.", args.name)
            return 1

        time.sleep(5)


if __name__ == "__main__":
    sys.exit(main())
