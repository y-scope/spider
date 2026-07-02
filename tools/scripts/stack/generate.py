#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["pyyaml>=6.0"]
# ///
"""
Generate per-service Spider configs from a single global config.

Reads the global ``spider.yaml`` and writes three serde-shaped per-service YAML files
(``gen-storage.yaml``, ``gen-scheduler.yaml``, ``gen-em.yaml``) that are each passed to their
Rust binary via ``--config``. Shared values (MariaDB, gRPC endpoints, binary/run/package paths)
are factored into the global config and defined once; this script derives each binary's exact
serde schema from them.

Runnable standalone for debugging::

    uv run --script tools/scripts/stack/generate.py
    uv run --script tools/scripts/stack/generate.py --config path/to/spider.yaml --output-dir /tmp
"""

import argparse
import logging
import sys
from pathlib import Path

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Top-level fields the global config must provide. Missing any of these is a fail-fast error.
_REQUIRED_TOP_LEVEL = (
    "binary_dir",
    "package_dir",
    "run_dir",
    "mariadb",
    "storage_endpoint",
    "scheduler_endpoint",
    "storage",
    "scheduler",
    "execution_manager",
)


# ``yaml_serde`` (used by the Rust binaries) deserializes serde externally-tagged enums with a
# YAML ``!tag``. Register the scheduler's ``!round_robin`` variant on the safe loader so this
# script can read the global config, plus a marker dict + representer so it can emit it back into
# the generated scheduler config.
class _RoundRobin(dict):
    """Mapping rendered as a ``!round_robin`` YAML tag for ``yaml_serde``."""


yaml.SafeLoader.add_constructor(
    "!round_robin",
    lambda loader, node: loader.construct_mapping(node, deep=True),
)
yaml.SafeDumper.add_representer(
    _RoundRobin,
    lambda dumper, data: dumper.represent_mapping("!round_robin", data),
)


def _resolve(path: str) -> Path:
    """Resolve ``path`` relative to the current working directory (absolute paths pass through)."""
    return Path(path).resolve() if Path(path).is_absolute() else (Path.cwd() / path).resolve()


def _load_yaml(path: Path) -> dict:
    """Load a YAML file with the ``!round_robin`` tag registered on the safe loader."""
    with path.open() as file:
        return yaml.load(file, Loader=yaml.SafeLoader)


def _require(mapping: dict, key: str, where: str) -> object:
    """Return ``mapping[key]`` or fail fast with a clear message about the missing field."""
    if key not in mapping:
        logger.error("Missing required field '%s' in %s.", key, where)
        sys.exit(1)
    return mapping[key]


def _build_storage_config(config: dict) -> dict:
    """Derive ``spider_storage_grpc_server``'s ``ServerConfig`` from the global config."""
    mariadb = config["mariadb"]
    storage = config["storage"]
    endpoint = config["storage_endpoint"]
    runtime: dict = {
        "db_config": {
            "host": _require(mariadb, "host", "mariadb"),
            "port": _require(mariadb, "port", "mariadb"),
            "name": _require(mariadb, "database", "mariadb"),
            "username": _require(mariadb, "username", "mariadb"),
            "password": _require(mariadb, "password", "mariadb"),
            "max_connections": _require(storage, "max_connections", "storage"),
        },
    }
    # These three sub-configs have #[serde(default)] in the Rust schema; only emit the ones the
    # global config actually sets (the rest are commented out) so serde applies its own defaults.
    for optional in ("ready_queue_config", "task_instance_pool_config", "job_cache_gc_config"):
        if optional in storage:
            runtime[optional] = storage[optional]
    return {
        "host": _require(endpoint, "host", "storage_endpoint"),
        "port": _require(endpoint, "port", "storage_endpoint"),
        "runtime": runtime,
    }


def _build_scheduler_config(config: dict) -> dict:
    """Derive ``spider_scheduler_grpc_server``'s ``ServerConfig`` from the global config."""
    scheduler = config["scheduler"]
    runtime = _require(scheduler, "runtime", "scheduler")
    storage_endpoint = config["storage_endpoint"]
    scheduler_endpoint = config["scheduler_endpoint"]
    out_runtime: dict = {
        "scheduler": _RoundRobin(_require(runtime, "scheduler", "scheduler.runtime")),
        "host": _require(scheduler_endpoint, "host", "scheduler_endpoint"),
        "port": _require(scheduler_endpoint, "port", "scheduler_endpoint"),
    }
    # em_registry and stop_timeout_sec have #[serde(default)]; only emit if set.
    if "em_registry" in runtime:
        out_runtime["em_registry"] = runtime["em_registry"]
    if "stop_timeout_sec" in runtime:
        out_runtime["stop_timeout_sec"] = runtime["stop_timeout_sec"]
    return {
        "storage_endpoint": {
            "host": _require(storage_endpoint, "host", "storage_endpoint"),
            "port": _require(storage_endpoint, "port", "storage_endpoint"),
        },
        "storage_connection_pool_size": _require(
            scheduler,
            "storage_connection_pool_size",
            "scheduler",
        ),
        "runtime": out_runtime,
    }


def _build_em_config(config: dict) -> dict:
    """Derive ``spider_execution_manager``'s ``Config`` from the global config."""
    em = config["execution_manager"]
    storage_endpoint = config["storage_endpoint"]
    scheduler_endpoint = config["scheduler_endpoint"]
    return {
        "host": _require(em, "host", "execution_manager"),
        "storage": {
            "host": _require(storage_endpoint, "host", "storage_endpoint"),
            "port": _require(storage_endpoint, "port", "storage_endpoint"),
        },
        "scheduler": {
            "host": _require(scheduler_endpoint, "host", "scheduler_endpoint"),
            "port": _require(scheduler_endpoint, "port", "scheduler_endpoint"),
        },
        "liveness": _require(em, "liveness", "execution_manager"),
        # bin_path / log_dir are derived from the shared binary_dir / run_dir so they stay
        # consistent with where run.py actually puts the binaries and logs. They are relative
        # strings, resolved by the execution manager against its own working directory.
        "task_executor": {
            "bin_path": f"{config['binary_dir']}/spider-task-executor",
            "package_dir": config["package_dir"],
            "log_dir": f"{config['run_dir']}/em-logs",
        },
        "connection_pool_size": _require(em, "connection_pool_size", "execution_manager"),
        "scheduler_poll_wait_ms": _require(em, "scheduler_poll_wait_ms", "execution_manager"),
    }


def main() -> int:
    """Generate the three per-service configs and write them to the output directory."""
    parser = argparse.ArgumentParser(
        description="Generate per-service Spider configs from a single global config.",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="tools/scripts/stack/spider.yaml",
        help="Path to the global stack config (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory to write generated configs (default: the config's run_dir)",
    )
    args = parser.parse_args()

    config = _load_yaml(_resolve(args.config))
    for section in _REQUIRED_TOP_LEVEL:
        if section not in config:
            logger.error("Missing required top-level field '%s' in global config.", section)
            sys.exit(1)

    output_dir = _resolve(args.output_dir) if args.output_dir else _resolve(config["run_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    services = {
        "gen-storage.yaml": _build_storage_config(config),
        "gen-scheduler.yaml": _build_scheduler_config(config),
        "gen-em.yaml": _build_em_config(config),
    }
    for filename, data in services.items():
        path = output_dir / filename
        with path.open("w") as file:
            yaml.dump(
                data,
                file,
                Dumper=yaml.SafeDumper,
                default_flow_style=False,
                sort_keys=False,
            )
        logger.info("Wrote %s.", path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
