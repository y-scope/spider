# Running the Spider stack

`tools/scripts/stack/run.py` brings up the whole Spider service stack locally -- a MariaDB
container plus the storage, scheduler, and N execution-manager Rust binaries -- in dependency
order and supervises them in the foreground. It is a standalone `uv` script, so it has no
install step beyond [uv] itself.

## Prerequisites

* The Rust release binaries must already be built. From the repo root:

  ```shell
  task build:rust
  ```

  The binaries land in `build/rust-targets/release/` (the workspace's `CARGO_TARGET_DIR`). The
  run script fails fast if any required binary is missing.
* The compiled task libraries must be staged so task executors can `dlopen` them. From the repo
  root:

  ```shell
  task build:packages
  ```

  This builds the workspace and copies each task library into
  `build/tdl_packages/<package>/lib<package>.so` (the `package_dir` configured in `spider.yaml`).
  Run it after `task build:rust`. It is only needed when the jobs you submit reference a task
  package (e.g. the `complex` package used by the example client below).
* [Docker] must be runnable by your user (used to start the MariaDB container).

## Configuration

There is one hand-written config, `tools/scripts/stack/spider.yaml` -- the single source of truth
for the whole stack. It factors shared values (MariaDB, gRPC endpoints, binary/run/package paths)
to the top and gives each service its own section listing every knob for that binary; knobs with
`#[serde(default)]` in the Rust schema are commented out so serde applies its own defaults.

Each Rust binary still consumes its own `--config <file>` with its own serde schema, so at launch
`run.py` calls `tools/scripts/stack/generate.py` to derive three per-service configs from
`spider.yaml` and write them into `run_dir`:

| Generated file       | Consumed by                   |
|----------------------|-------------------------------|
| `gen-storage.yaml`   | `spider_storage_grpc_server`  |
| `gen-scheduler.yaml` | `spider_scheduler_grpc_server`|
| `gen-em.yaml`        | `spider_execution_manager`    |

`run.py` then passes each generated file to its binary via `--config`. The generated files are
kept on disk (under the gitignored `build/` tree) for debugging -- inspect them to see exactly
what each binary was handed. Do not edit them; edit `spider.yaml` and regenerate.

`generate.py` is also runnable standalone, which is the easiest way to check what the derivation
produces without launching the stack:

```shell
uv run --script tools/scripts/stack/generate.py
uv run --script tools/scripts/stack/generate.py --output-dir /tmp
```

Paths in `spider.yaml` are resolved relative to the current working directory `run.py` is
launched from. The defaults assume that is the repository root.

To run more tasks in parallel, increase the worker count -- each execution manager runs one
task-executor at a time. Either edit `workers:` in `spider.yaml` or pass `--workers`.

> **Note:** `spider.yaml` writes the round-robin scheduler as `!round_robin` (a YAML tag), not as
> a `{ round_robin: { ... } }` map. The `yaml_serde` crate the server uses deserializes serde
> externally-tagged enums this way; a plain map will fail to parse. `generate.py` registers a
> matching representer so the generated `gen-scheduler.yaml` emits the same tag.

## Running the stack

```shell
uv run --script tools/scripts/stack/run.py
```

This generates the per-service configs, starts MariaDB, waits for it to accept connections, then
starts storage, waits for its port (`50051`), starts the scheduler, waits for its port (`50052`),
then launches the configured number of execution-manager workers. Once everything is up, it
supervises the services in the foreground.

Useful flags:

| Flag                | Default                     | Description                                                      |
|---------------------|-----------------------------|------------------------------------------------------------------|
| `--config`          | `tools/scripts/stack/spider.yaml` | Path to the global stack config.                          |
| `--workers N`       | from config                 | Override the execution-manager worker count.                     |
| `--skip-mariadb`    | off                         | Assume MariaDB is already running; do not start it.             |
| `--teardown`        | off                         | Also stop the MariaDB container when the run ends.              |
| `--start-timeout S` | `30`                        | Seconds to wait for each service to become ready.                |

## Stopping

Press `Ctrl-C` (or send `SIGTERM`) to stop the run. Services are torn down in reverse launch
order (execution managers -> scheduler -> storage); any that do not exit in time are `SIGKILL`ed.

By default the MariaDB container is **left running** between runs so database state persists.
Pass `--teardown` to also stop and remove the MariaDB container when the run ends:

```shell
uv run --script tools/scripts/stack/run.py --teardown
```

## Logs and generated configs

`build/spider-run/` holds the runtime artifacts:

* `gen-storage.yaml`, `gen-scheduler.yaml`, `gen-em.yaml` -- the generated per-service configs.
* `storage.log`, `scheduler.log`, `em-0.log`, `em-1.log`, ... -- per-service stdout/stderr
  (truncated on each launch).
* `em-logs/<em_id>-<executor_id>.log` -- per task-executor subprocess logs.

This directory lives under the gitignored `build/` tree, so none of it is committed.

## Example: a layered task graph with `huntsman-complex-client`

`examples/huntsman/complex/client` is a small client binary that builds a "neural-network-shaped"
task graph out of the `complex` package's `complex::add` task and runs it against a live stack. The
graph has `--level` layers of `--width` tasks each: layer 0 takes its two inputs from the graph
inputs, and every inner task adds two outputs from the previous layer. Tasks within a layer are
independent, so `--width` controls how much parallelism the scheduler can exploit. After the job
finishes, the client decodes the final layer's outputs and checks them against an in-process
simulation of the same DAG, so a successful run proves the stack executed the graph correctly.

The binary is built by `task build:packages` (which builds the whole workspace, including the
example crates) and lands in `build/rust-targets/release/huntsman-complex-client`. Run it against a
stack that is already up. In one shell, start the stack with enough workers to run a layer in
parallel:

```shell
uv run --script tools/scripts/stack/run.py --workers 16
```

In another shell, once the stack reports it is up, run the client:

```shell
build/rust-targets/release/huntsman-complex-client --level 10 --width 16
```

A width-16 graph keeps all 16 execution managers busy within each layer. On success the client
prints that every final-layer output matched the local simulation. Defaults are `--level 10` and
`--width 4`; pass `--help` for the full flag list. This example was validated with `--workers 16`
and `--width 16`.

## Notes

* The storage service creates its own database tables on connect (`CREATE TABLE IF NOT
  EXISTS`), so no schema initialization step is needed or run. Do not pre-create tables from
  the older `tools/scripts/mariadb/wolf/` schema -- a stale schema will cause storage to fail
  with column-mismatch errors.
* The MariaDB container is started with `--rm`, so stopping it also discards its data -- each
  `--teardown` run starts from a fresh database.

[uv]: https://docs.astral.sh/uv/
[Docker]: https://docs.docker.com/get-started/