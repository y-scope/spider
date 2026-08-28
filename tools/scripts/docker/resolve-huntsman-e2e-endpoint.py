#!/usr/bin/env -S uv run --script
# /// script
# dependencies = []
# ///
"""
Script to resolve the endpoint the Huntsman e2e test driver should use to reach `spider-storage`.

Self-hosted runners mount the host's Docker socket, so containers started by `docker compose` are
siblings on the host rather than children of the runner: a port `docker compose` publishes lands in
the host's network namespace, unreachable from the runner. When that's detected, `connect` instead
joins the runner into the Compose stack's own network, so its embedded DNS resolves `spider-storage`
directly and host-port publishing is bypassed entirely. `disconnect` undoes that join during
cleanup, before the network itself is removed.

GitHub-hosted runners and local dev machines aren't containers on the daemon's own host, so
detection fails there. `connect` then resolves to `127.0.0.1:<published-port>`, the host port
`docker compose` publishes `spider-storage` on, and `disconnect` becomes a no-op.
"""

import argparse
import logging
import socket
import subprocess
import sys
from pathlib import Path

_DOCKER_ENV_FILE = Path("/.dockerenv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _resolve_runner_id(docker_executable: str) -> str:
    """
    Resolves the ID of the container this script is running in.

    Docker defaults a container's hostname to its ID, and Compose sets it to the container's name;
    the daemon resolves either.

    :param docker_executable: The Docker executable to invoke.
    :return: The container's ID.
    :return: An empty string (not an error) when we're not running inside a container the daemon
        can resolve, e.g. on a GitHub-hosted runner or a dev machine.
    """
    if not _DOCKER_ENV_FILE.is_file():
        return ""

    result = subprocess.run(
        [docker_executable, "inspect", "-f", "{{.Id}}", socket.gethostname()],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return ""

    return result.stdout.strip()


def _connect(
    docker_executable: str,
    runner_id: str,
    network: str,
    storage_port: int,
    published_port: int,
) -> int:
    """
    Prints the endpoint to reach `spider-storage` at, joining `network` first if necessary.

    :param docker_executable: The Docker executable to invoke.
    :param runner_id: The ID of the container this script is running in, or an empty string.
    :param network: The Compose stack's network.
    :param storage_port: The port `spider-storage` listens on inside `network`.
    :param published_port: The host port `spider-storage` is published on.
    :return: 0 on success.
    :return: The Docker exit code on failure.
    """
    # ruff: noqa: T201
    if not runner_id:
        print(f"http://127.0.0.1:{published_port}")
        return 0

    result = subprocess.run(
        [docker_executable, "network", "connect", network, runner_id],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        logger.error("Failed to join network '%s':\n%s", network, result.stderr)
        return result.returncode

    print(f"http://spider-storage:{storage_port}")
    return 0


def _disconnect(docker_executable: str, runner_id: str, network: str) -> int:
    """
    Undoes the join performed by `_connect`.

    :param docker_executable: The Docker executable to invoke.
    :param runner_id: The ID of the container this script is running in, or an empty string.
    :param network: The Compose stack's network.
    :return: 0.
    """
    if runner_id:
        # Safe to ignore failures: this is a no-op if `connect` never ran (e.g. GitHub-hosted
        # runners and dev machines), and cleanup shouldn't fail the task over it either way.
        _ = subprocess.run(
            [docker_executable, "network", "disconnect", network, runner_id],
            capture_output=True,
            text=True,
            check=False,
        )

    return 0


def main() -> int:
    """Main."""
    parser = argparse.ArgumentParser(
        description="Resolve the endpoint the Huntsman e2e test driver uses to reach spider-storage"
    )
    subparsers = parser.add_subparsers(dest="mode", required=True)

    connect_parser = subparsers.add_parser(
        "connect", help="Print the endpoint, joining the Compose network if necessary."
    )
    connect_parser.add_argument(
        "--network",
        type=str,
        required=True,
        help="The Compose stack's network",
    )
    connect_parser.add_argument(
        "--storage-port",
        type=int,
        required=True,
        help="The port spider-storage listens on inside the Compose network",
    )
    connect_parser.add_argument(
        "--published-port",
        type=int,
        required=True,
        help="The host port spider-storage is published on",
    )

    disconnect_parser = subparsers.add_parser(
        "disconnect", help="Leave the Compose network if it was joined by `connect`."
    )
    disconnect_parser.add_argument(
        "--network",
        type=str,
        required=True,
        help="The Compose stack's network",
    )

    args = parser.parse_args()

    # Silence Ruff S607: the absolute path of the Docker binary may vary depending on the
    # installation method.
    docker_executable = "docker"

    runner_id = _resolve_runner_id(docker_executable)

    if "connect" == args.mode:
        return _connect(
            docker_executable, runner_id, args.network, args.storage_port, args.published_port
        )
    if "disconnect" == args.mode:
        return _disconnect(docker_executable, runner_id, args.network)

    logger.error("Unknown mode: '%s'. Expected 'connect' or 'disconnect'.", args.mode)
    return 1


if __name__ == "__main__":
    sys.exit(main())
