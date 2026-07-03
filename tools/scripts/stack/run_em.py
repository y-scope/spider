#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["pyyaml>=6.0"]
# ///
"""
Run Spider execution-manager workers only.

Used to run task-executor workers on a node separate from the storage/scheduler services (e.g. when
distributing workers across multiple nodes). Launches the configured number of execution managers
from the global stack config; each spawns a task-executor and registers with the scheduler at
``scheduler_endpoint``. The storage and scheduler services must already be running -- start them
on the scheduler node via ``run.py`` first.

The per-service EM config is generated from the global config by ``generate.py`` at launch (only
``gen-em.yaml`` is consumed here; the storage/scheduler configs it also writes are unused on a
worker node). Ctrl-C / SIGTERM tears down the workers in reverse launch order.

For multi-node deployments, point the global config's ``storage_endpoint``/``scheduler_endpoint``
at the scheduler/storage node and set ``execution_manager.host`` to this node's reachable IP before
running this script. Workers self-differentiate by a generated execution-manager ID, so launching
several EMs from one config on one node does not collide.

The Rust binaries must already be built -- run ``task build:rust`` first. The script fails fast if
any required binary is missing.
"""

import argparse
import contextlib
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import yaml

# To silence Ruff S607: the absolute path of this executable may vary depending on the
# installation method.
_uv_executable = "uv"

# generate.py lives next to this script and derives the per-service configs from the global
# config; locate it relative to this file rather than the current working directory.
_STACK_SCRIPTS_DIR = Path(__file__).resolve().parent

# Only the execution manager and the task-executor it spawns are needed on a worker node.
_REQUIRED_BINARIES = (
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


def _generate_configs(global_config: Path, run_dir: Path) -> None:
    """
    Run ``generate.py`` to materialize the per-service configs into ``run_dir``.

    Only ``gen-em.yaml`` is consumed here; generate.py also writes the storage/scheduler configs,
    which are harmless on a worker node.
    """
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


def _teardown() -> None:
    """SIGTERMs every running worker in reverse launch order, then SIGKILLs stragglers."""
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
    logger.info("All workers stopped.")


def _on_signal(_signum: int, _frame: object) -> None:
    """Sets the exit event so the supervise loop breaks and the finally block tears down."""
    _exit_event.set()


def _parse_args() -> argparse.Namespace:
    """Builds and parses the command-line arguments for the worker launcher."""
    parser = argparse.ArgumentParser(description="Run Spider execution-manager workers only.")
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
    return parser.parse_args()


def main() -> int:
    """Main."""
    args = _parse_args()

    global_config = _resolve(args.config)
    config = _load_yaml(global_config)
    workers = args.workers if args.workers is not None else config["workers"]
    log_level = args.log_level if args.log_level is not None else config.get("log_level", "info")
    logger.info("Log level: %s", log_level)
    binary_dir = _resolve(config["binary_dir"])
    run_dir = _resolve(config["run_dir"])
    run_dir.mkdir(parents=True, exist_ok=True)

    _check_binaries(binary_dir)
    _generate_configs(global_config, run_dir)

    em_cfg_path = run_dir / "gen-em.yaml"
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
    logger.info("Workers are up. Press Ctrl-C to stop.")

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
        _teardown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
