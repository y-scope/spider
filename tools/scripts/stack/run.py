#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["pyyaml>=6.0"]
# ///
"""
Run the Spider stack: MariaDB, storage, scheduler, and N execution managers.

Services are launched in dependency order (storage -> scheduler -> execution managers), and each
is waited on until it accepts connections before the next is started. The script then supervises
the services in the foreground.

Per-service configs are generated from a single global config (``spider.yaml``) by ``generate.py``
at launch and written into ``run_dir``; each binary is then passed its generated file via
``--config``.

Ctrl-C / SIGTERM tears down the services in reverse order. MariaDB is left running by default so
database state persists across run cycles; pass ``--teardown`` to also stop the MariaDB container
when the run ends.

The Rust binaries must already be built -- run ``task build:rust`` first. The script fails fast
if any required binary is missing.
"""

import argparse
import contextlib
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import yaml

# To silence Ruff S607: the absolute path of this executable may vary depending on the
# installation method.
_uv_executable = "uv"

# The MariaDB helper scripts live next to this script (under tools/scripts/mariadb), so locate
# them relative to this file rather than the current working directory.
_MARIADB_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "mariadb"

# generate.py lives next to this script and derives the per-service configs from the global
# config; locate it relative to this file rather than the current working directory.
_STACK_SCRIPTS_DIR = Path(__file__).resolve().parent

_REQUIRED_BINARIES = (
    "spider_storage_grpc_server",
    "spider_scheduler_grpc_server",
    "spider_execution_manager",
    "spider-task-executor",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# (role, Popen) pairs, in launch order. Torn down in reverse.
_procs: list[tuple[str, subprocess.Popen]] = []
# Set by the signal handler so the supervise loop breaks and the finally block tears down.
_exit_event = threading.Event()

# ``yaml_serde`` (used by the Rust binaries) deserializes serde externally-tagged enums with a
# YAML ``!tag``. The global config contains the scheduler's ``!round_robin`` tag, so register it
# on the safe loader to let this script read the global config with PyYAML.
yaml.SafeLoader.add_constructor(
    "!round_robin",
    lambda loader, node: loader.construct_mapping(node, deep=True),
)


def _resolve(path: str) -> Path:
    """Resolves a path from the config relative to the current working directory."""
    return Path(path).resolve() if Path(path).is_absolute() else (Path.cwd() / path).resolve()


def _load_yaml(path: Path) -> dict:
    """Loads a YAML config file with the ``!round_robin`` tag registered on the safe loader."""
    with path.open() as file:
        return yaml.load(file, Loader=yaml.SafeLoader)


def _port_open(host: str, port: int) -> bool:
    """Returns whether ``host:port`` currently accepts a TCP connection."""
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


def _wait_for_port(host: str, port: int, timeout: float) -> bool:
    """Blocks until ``host:port`` accepts a TCP connection or ``timeout`` elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _port_open(host, port):
            return True
        time.sleep(0.5)
    return False


def _check_binaries(binary_dir: Path) -> None:
    """Fails fast if any required release binary is missing."""
    missing = [name for name in _REQUIRED_BINARIES if not (binary_dir / name).exists()]
    if missing:
        logger.error(
            "Missing binaries in %s: %s. Run `task build:rust` first.",
            binary_dir,
            ", ".join(missing),
        )
        sys.exit(1)


def _ensure_mariadb(mariadb: dict, skip: bool) -> None:
    """
    Starts the MariaDB container if it is not already running.

    The container creates the configured database on first start; the storage service creates
    its own tables on connect, so no schema initialization is needed here.
    """
    if skip:
        logger.info("Skipping MariaDB startup (--skip-mariadb).")
        return

    common_args = [
        "--port",
        str(mariadb["port"]),
        "--username",
        mariadb["username"],
        "--password",
        mariadb["password"],
        "--database",
        mariadb["database"],
    ]

    start_cmd = [
        _uv_executable,
        "run",
        "--script",
        str(_MARIADB_SCRIPTS_DIR / "start.py"),
        "--name",
        mariadb["name"],
        *common_args,
    ]
    result = subprocess.run(start_cmd, check=False)
    # mariadb/start.py returns 1 when the container already exists; treat that as success.
    if result.returncode == 1:
        logger.info("MariaDB container %s already running.", mariadb["name"])
    elif result.returncode != 0:
        logger.error("Failed to start MariaDB container (exit %d).", result.returncode)
        sys.exit(1)
    logger.info("MariaDB is ready.")


def _stop_mariadb(name: str) -> None:
    """Stops the MariaDB container via the existing mariadb script."""
    result = subprocess.run(
        [_uv_executable, "run", "--script", str(_MARIADB_SCRIPTS_DIR / "stop.py"), "--name", name],
        check=False,
    )
    if result.returncode != 0:
        logger.error("MariaDB stop script exited with code %d.", result.returncode)
    else:
        logger.info("MariaDB container stopped.")


def _generate_configs(global_config: Path, run_dir: Path) -> None:
    """Run ``generate.py`` to materialize the per-service configs into ``run_dir``."""
    result = subprocess.run(
        [
            _uv_executable,
            "run",
            "--script",
            str(_STACK_SCRIPTS_DIR / "generate.py"),
            "--config",
            str(global_config),
            "--output-dir",
            str(run_dir),
        ],
        check=False,
    )
    if result.returncode != 0:
        logger.error("generate.py failed (exit %d).", result.returncode)
        sys.exit(1)
    logger.info("Generated per-service configs in %s.", run_dir)


def _launch(role: str, args: list[str], log_file: Path, log_level: str) -> subprocess.Popen:
    """Launches a service process in a new session and tees its stderr to a log file."""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    # Truncate per launch so each run's log reflects only the current attempt, not stale output
    # from earlier failed runs.
    log = log_file.open("wb")
    # The Rust services read their log level from RUST_LOG; inject the resolved level so the stack
    # is observable without forcing the caller to set the env var. Any value already present in
    # os.environ is overwritten -- the config/CLI value is the single source of truth. The
    # task-executor child processes inherit this env from their execution manager, so the level
    # propagates to every binary in the stack.
    env = {**os.environ, "RUST_LOG": log_level}
    proc = subprocess.Popen(
        args,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        env=env,
    )
    _procs.append((role, proc))
    logger.info("Started %s (pid %d): %s", role, proc.pid, " ".join(args))
    return proc


def _teardown(mariadb: dict, stop_mariadb: bool) -> None:
    """SIGTERMs every running service in reverse launch order, then SIGKILLs stragglers."""
    for role, proc in reversed(_procs):
        if proc.poll() is not None:
            continue
        logger.info("Stopping %s (pid %d).", role, proc.pid)
        with contextlib.suppress(ProcessLookupError):
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)

    deadline = time.monotonic() + 10.0
    for role, proc in reversed(_procs):
        if proc.poll() is not None:
            continue
        remaining = max(0.0, deadline - time.monotonic())
        try:
            proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            logger.warning("%s did not exit in time; sending SIGKILL.", role)
            with contextlib.suppress(ProcessLookupError):
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    logger.info("All services stopped.")

    if stop_mariadb:
        _stop_mariadb(mariadb["name"])
    else:
        logger.info("Leaving MariaDB running. Pass --teardown to stop it on exit.")


def _on_signal(_signum: int, _frame: object) -> None:
    """Sets the exit event so the supervise loop breaks and the finally block tears down."""
    _exit_event.set()


def _parse_args() -> argparse.Namespace:
    """Builds and parses the command-line arguments for the stack runner."""
    parser = argparse.ArgumentParser(description="Run the Spider stack.")
    parser.add_argument(
        "--config",
        type=str,
        default="tools/scripts/stack/spider.yaml",
        help="Path to the top-level stack config (default: %(default)s)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Override the worker count from the config",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=None,
        help="Override the RUST_LOG level from the config (e.g. info, debug)",
    )
    parser.add_argument(
        "--skip-mariadb",
        action="store_true",
        help="Assume MariaDB is already running and initialized; do not start it",
    )
    parser.add_argument(
        "--teardown",
        action="store_true",
        help="Also stop the MariaDB container when the run ends",
    )
    parser.add_argument(
        "--start-timeout",
        type=float,
        default=30.0,
        help="Seconds to wait for each service to become ready (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> int:
    """Main."""
    args = _parse_args()

    global_config = _resolve(args.config)
    config = _load_yaml(global_config)
    mariadb = config["mariadb"]
    workers = args.workers if args.workers is not None else config["workers"]
    log_level = args.log_level if args.log_level is not None else config.get("log_level", "info")
    logger.info("Log level: %s", log_level)
    binary_dir = _resolve(config["binary_dir"])
    run_dir = _resolve(config["run_dir"])
    run_dir.mkdir(parents=True, exist_ok=True)
    storage_endpoint = config["storage_endpoint"]
    scheduler_endpoint = config["scheduler_endpoint"]

    _check_binaries(binary_dir)
    _generate_configs(global_config, run_dir)
    _ensure_mariadb(mariadb, args.skip_mariadb)

    storage_cfg_path = run_dir / "gen-storage.yaml"
    scheduler_cfg_path = run_dir / "gen-scheduler.yaml"
    em_cfg_path = run_dir / "gen-em.yaml"

    storage_args = [
        str(binary_dir / "spider_storage_grpc_server"),
        "--config",
        str(storage_cfg_path),
    ]
    _launch("storage", storage_args, run_dir / "storage.log", log_level)
    if not _wait_for_port(
        str(storage_endpoint["host"]),
        storage_endpoint["port"],
        args.start_timeout,
    ):
        logger.error("Storage did not become ready in %ss.", args.start_timeout)
        _teardown(mariadb, args.teardown)
        return 1
    logger.info("Storage is ready.")

    scheduler_args = [
        str(binary_dir / "spider_scheduler_grpc_server"),
        "--config",
        str(scheduler_cfg_path),
    ]
    _launch("scheduler", scheduler_args, run_dir / "scheduler.log", log_level)
    if not _wait_for_port(
        str(scheduler_endpoint["host"]),
        scheduler_endpoint["port"],
        args.start_timeout,
    ):
        logger.error("Scheduler did not become ready in %ss.", args.start_timeout)
        _teardown(mariadb, args.teardown)
        return 1
    logger.info("Scheduler is ready.")

    em_args = [
        str(binary_dir / "spider_execution_manager"),
        "--config",
        str(em_cfg_path),
    ]
    for i in range(workers):
        _launch(f"em-{i}", em_args, run_dir / f"em-{i}.log", log_level)
        # Give each EM a moment to register before launching the next, so the scheduler
        # sees them arrive in order.
        time.sleep(1.0)
    logger.info("Launched %d execution-manager worker(s).", workers)
    logger.info("Stack is up. Press Ctrl-C to stop.")

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)
    try:
        while not _exit_event.is_set():
            for role, proc in _procs:
                if proc.poll() is not None:
                    logger.error("%s exited unexpectedly (code %d).", role, proc.returncode)
                    return 1
            _exit_event.wait(1.0)
    finally:
        _teardown(mariadb, args.teardown)
    return 0


if __name__ == "__main__":
    sys.exit(main())
