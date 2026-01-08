#!/usr/bin/env -S uv run --script
# /// script
# dependencies = []
# ///
"""Script to start a MariaDB Docker container."""

import argparse
import logging
import subprocess
import sys

_MARIADB_IMAGE = "mariadb:latest"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> int:
    """Main."""
    parser = argparse.ArgumentParser(description="Start MariaDB Docker container.")
    parser.add_argument(
        "--name",
        type=str,
        default="mariadb-spider-dev",
        help="The name of the started MariaDB Docker container (default: %(default)s)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=3306,
        help="The port to expose MariaDB on (default: %(default)d)",
    )
    parser.add_argument(
        "--username",
        type=str,
        default="spider-user",
        help="The username of the started MariaDB (default: %(default)s)",
    )
    parser.add_argument(
        "--password",
        type=str,
        default="spider-password",
        help="The password of the started MariaDB (default: %(default)s)",
    )
    parser.add_argument(
        "--database",
        type=str,
        default="spider-db",
        help="The database name of the started MariaDB (default: %(default)s)",
    )
    args = parser.parse_args()

    # Silence Ruff S607: the absolute path of the Docker binary may vary depending on the
    # installation method.
    docker_executable = "docker"

    result = subprocess.run(
        [docker_executable, "inspect", "-f", "{{.State.Running}}", args.name],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0 and result.stdout.rstrip("\n") == "true":
        logger.info("Container %s already exists.", args.name)
        return 1

    logger.info("Starting MariaDB container %s on port %d.", args.name, args.port)
    logger.info("Pulling latest Mariadb image.")
    result = subprocess.run(
        [docker_executable, "pull", _MARIADB_IMAGE],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        logger.error("Failed to pull MariaDB image:\n%s", result.stderr)
        return result.returncode
    logger.info("Successfully pulled latest MariaDB image.")

    mariadb_start_cmd = [
        docker_executable,
        "run",
        "--rm",
        "-d",
        "--name",
        args.name,
        "-e",
        f"MARIADB_USER={args.username}",
        "-e",
        f"MARIADB_PASSWORD={args.password}",
        "-e",
        f"MARIADB_DATABASE={args.database}",
        "-e",
        f"MARIADB_ROOT_PASSWORD={args.password}",
        "-p",
        f"{args.port}:3306",
        _MARIADB_IMAGE,
    ]

    result = subprocess.run(
        mariadb_start_cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        logger.error("Failed to start MariaDB container:\n%s", result.stderr)
        return result.returncode
    logger.info("MariaDB container started successfully with ID: %s.", result.stdout.strip())
    return 0


if __name__ == "__main__":
    sys.exit(main())
