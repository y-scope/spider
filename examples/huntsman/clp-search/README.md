# CLP-search-over-Spider benchmark harness

A small benchmark harness that runs a **multi-worker CLP search** over a directory of CLP archives
by fanning the work out across a live Spider stack. Each archive is searched by an independent
Spider task that shells out to the `clp-s` binary; the client collects and prints the results.

It exists to benchmark Spider on a real workload (embarrassingly-parallel search over hundreds of
archives) and to compare it against non-Spider baselines.

## Crates

| Path        | Crate / artifact                     | What it is                                                        |
|-------------|--------------------------------------|-------------------------------------------------------------------|
| `client/`   | `huntsman-clp-search-client` (bin)   | The harness: builds a Spider job from a query + archive dir, runs it, prints results. |
| `tasks/`    | `huntsman-clp-search-tasks` (cdylib) | The TDL package `clp_search` exposing the single task `clp_search::search`. Staged as `libclp_search.so`. |
| `pool-ref/` | `huntsman-clp-search-pool-ref` (bin) | Baseline: the same fan-out + `clp-s` work through a local process pool, **no Spider** — used to isolate Spider's scheduling overhead. |

## Prerequisites

* The `clp-s` binary. Defaults to `/home/lzh/dev/clp/build/core/clp-s`; override with the
  `CLP_S_BIN` environment variable (read by the task and the pool-ref binary).
* A directory of CLP archives, where **every immediate subdirectory is one archive** (e.g.
  `~/dev/clp/build/clp-package/var/data/archives/default`).
* The workspace built and the `clp_search` package staged so task executors can `dlopen` it:

  ```shell
  task build:rust
  task build:packages
  ```

  `build:packages` builds the example binaries and stages `libclp_search.so` into
  `build/tdl_packages/clp_search/`.
* A running Spider stack. See `../../../stack-doc.md`. The stack must have the `clp_search` package
  staged (the step above) before you submit a job that references it.

## Running

In one shell, bring up the stack (defaults are 16 workers and a large scheduler ready-task queue,
which the concurrent-job case needs):

```shell
uv run --script tools/scripts/stack/run.py
```

In another shell, once the stack reports it is up, run the client:

```shell
build/rust-targets/release/huntsman-clp-search-client \
  --input ~/dev/clp/build/clp-package/var/data/archives/default \
  --query '*NonDFS*'
```

### Client arguments

| Flag           | Default                               | Meaning                                                        |
|----------------|---------------------------------------|----------------------------------------------------------------|
| `--input`      | (required)                            | Directory whose immediate subdirectories are CLP archives.     |
| `--query`      | (required)                            | The KQL search query, applied to every archive.                |
| `--endpoint`   | `http://127.0.0.1:50051`              | Spider storage gRPC endpoint.                                  |
| `--pool-size`  | `4`                                   | `SpiderClient` gRPC connection-pool size.                      |
| `--output-dir` | `build/spider-run/clp-search-results` | Base dir; each run creates a unique `run-<nanos>` subdir.      |
| `--no-shuffle` | off (archives are shuffled)           | Submit archives in sorted order instead of shuffling them.     |

By default the client **shuffles** the archive order before submission, so heavy archives are spread
across workers rather than concentrated on one straggler. For a cheap query whose per-task cost is
uniform this mostly adds run-to-run variance (the assignment is random each run), so pass
`--no-shuffle` for reproducible measurements; keep the shuffle when tasks are expensive and a sorted
order would systematically pile heavy archives onto a few workers.

**Output convention:** search results (JSONL) go to **stdout**; the submit line and the per-phase
timing breakdown go to **stderr**. So `client ... > results.jsonl` captures just the results.

## How the client works

1. **Discover archives** — every immediate subdirectory of `--input`, canonicalized to an absolute
   path and sorted by name, then shuffled (unless `--no-shuffle`) to randomize task-to-worker
   assignment.
2. **Prepare outputs + graph** — create a unique run directory and assign each archive a unique
   output file `<run-dir>/<index>-<archive-name>.jsonl` (tasks never share an output path). Build a
   **flat** `TaskGraph`: one `clp_search::search` task per archive, each with three graph inputs
   (archive path, query, output path) and **no outputs**, and no inter-task dependencies — so the
   scheduler can run all of them in parallel across the execution managers.
3. **Build inputs** — flatten the per-task inputs in insertion/position order (archive path, query,
   output path), matching `TaskGraph::get_task_graph_input_indices`. Each string is msgpack-encoded
   into a `TaskInput::ValuePayload`; the graph declares each input as an opaque `bytes` type.
4. **Submit → start → poll** — register a per-run resource group, `submit_job`, `start_job`, then
   poll `get_job_state` (every 10 ms) until the job reaches a terminal state.
5. **Print results** — on success, read each per-task output file in archive order and concatenate
   them to stdout. On failure, fetch and report the job error.
6. **Report timing** — print a phase breakdown to stderr: `query_processing` (discovery + graph +
   inputs + connect + submit/start), `spider_execution` (the poll-to-terminal wait — this is where
   nearly all the wall-clock lives), and `post_processing` (reading + printing results).

## The search task (`clp_search::search`)

A single TDL task with signature `search(archive_path, query, output_path) -> Result<(), TdlError>`
(no output). It runs `clp-s s <archive_path> <query>` with the child's stdout redirected into
`output_path`, and returns an error if `clp-s` cannot be spawned or exits non-zero. The binary is
resolved from `CLP_S_BIN` (default `/home/lzh/dev/clp/build/core/clp-s`).

## Baseline (`pool-ref`)

`clp-search-pool-ref` runs the identical workload — same discovery, same per-archive output files,
same `clp-s s <archive> <query>` invocation — but through a local `tokio` process pool bounded by
`--pool-size` (default 16) instead of Spider. It emits the same phase-timing labels, so the gap
between its execution time and Spider's `spider_execution` is Spider's scheduling/coordination
overhead. Run it exactly like the client but without `--endpoint`:

```shell
build/rust-targets/release/clp-search-pool-ref \
  --input ~/dev/clp/build/clp-package/var/data/archives/default --query '*NonDFS*'
```

The pool-ref also prints a `[clp_s]` line to stderr with the per-archive `clp-s` execution
distribution (count/sum/mean/median/min/max/p95), directly comparable to Spider's `clp_s_elapsed_us`
metric below — useful for separating `clp-s` execution cost (and CPU contention) from Spider's
coordination overhead.

## Benchmark metrics

Beyond the client's own stderr phase breakdown (`query_processing` / `spider_execution` /
`post_processing`, with `submit_job` and `start_job` timed separately, and a
`client_job_start_epoch_us` line for the scheduling-overhead calc), the stack emits per-task metrics
as structured JSON logs when it runs at `info` (set `log_level: "info"` in `spider.yaml`, or
`RUST_LOG=info`). Collect them from `build/spider-run/`:

| Metric (JSON field) | Where | Meaning |
|---------------------|-------|---------|
| `scheduler_next_task_us`     | `em-*.log`             | EM time to fetch a task from the scheduler (excl. the first, idle long-poll). |
| `register_task_instance_us`  | `em-*.log`             | EM time to register the task instance in storage. |
| `task_executor_execute_us`   | `em-*.log`             | EM-side task-executor round-trip (includes `clp-s`). |
| `clp_s_elapsed_us`           | `em-logs/<em>-<x>.log` | `clp-s` subprocess wall time, measured inside the task. |
| `Dispatched a task assignment` (log timestamp) | `scheduler.log` | When the scheduler handed a task to an EM. |

Aggregate by filtering log lines on `job_id`. Scheduling overhead per job = the scheduler's first
dispatch timestamp minus the client's `client_job_start_epoch_us`.

## Notes

* Because everything runs on one host, the task processes and the client share a filesystem, so the
  client reads back exactly the files the tasks wrote. All paths are made absolute for this reason.
* Prefer a **freshly launched** stack per benchmark: a stack left running can drift into a bad state
  and fail on the next submission.
* The scheduler's `ready_task_capacity` (in `tools/scripts/stack/spider.yaml`) must exceed the total
  number of simultaneously-ready tasks when submitting **concurrent** jobs (N jobs × M archives).
  Otherwise concurrent jobs backpressure the scheduler and become slow and uneven. The default is
  sized generously for this.
