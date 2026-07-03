# Neural-network-shaped Spider benchmark

A small benchmark harness that runs a **layered, neural-network-shaped task graph** over a live
Spider stack. Each task is a simulated neuron -- it consumes 25 128-byte inputs, sleeps a fixed
10 ms to model compute cost, and emits a fixed 128-byte output. The graph is `--level` layers of
`--width` tasks each (default `--level 10 --width 1000` => 10,000 tasks), where every inner task
draws its 25 inputs from distinct random outputs of the previous layer. The 25 inputs per task is
fixed by the `nn_bench::sleep` task's signature, not a tunable.

It exists to benchmark Spider on a structured, dependency-heavy workload and to compare Spider's
actual execution time against an **analytic ideal-runtime lower bound** computed from the generated
graph shape.

## Crates

| Path        | Crate / artifact                  | What it is                                                        |
|-------------|-----------------------------------|-------------------------------------------------------------------|
| `client/`   | `huntsman-nn-bench` (bin)         | The harness: builds the layered `nn_bench::sleep` graph, runs it, verifies outputs, reports timing + ideal runtime. |
| `tasks/`    | `huntsman-nn-bench-tasks` (cdylib) | The TDL package `nn_bench` exposing the single task `nn_bench::sleep`. Staged as `libnn_bench.so`. |

## Prerequisites

* The workspace built and the `nn_bench` package staged so task executors can `dlopen` it:

  ```shell
  task build:rust
  task build:packages
  ```

  `build:packages` builds the example binaries and stages `libnn_bench.so` into
  `build/tdl_packages/nn_bench/`.
* A running Spider stack. See `../../../stack-doc.md`. The stack must have the `nn_bench` package
  staged (the step above) before you submit a job that references it.

## Running

In one shell, bring up the stack (defaults are 16 workers and a large scheduler ready-task queue,
which the concurrent-job case needs):

```shell
uv run --script tools/scripts/stack/run.py
```

In another shell, once the stack reports it is up, run the client:

```shell
build/rust-targets/release/huntsman-nn-bench --level 10 --width 32
```

A width-32 graph keeps all 16 execution managers busy within each layer (and the task's 25-input
signature requires `--width >= 25` so each task can draw distinct previous-layer outputs). For a
larger workload, raise `--width` (and the stack's `--workers` to match, so a layer can run in
parallel). The default shape (`--level 10 --width 1000`) is 10,000 tasks and is meant for a
many-worker stack.

### Client arguments

| Flag              | Default                  | Meaning                                                        |
|-------------------|--------------------------|----------------------------------------------------------------|
| `--endpoint`      | `http://127.0.0.1:50051` | Spider storage gRPC endpoint.                                  |
| `--level`         | `10`                     | Number of layers (graph depth).                                |
| `--width`         | `1000`                   | Tasks per layer; must be >= 25 (the task's fixed input count) so each task can sample distinct previous-layer outputs. Controls the parallelism the scheduler can exploit. |
| `--input-bytes`   | `128`                    | Size in bytes of each input and of each task's fixed output.   |
| `--seed`          | `0x517_d3ad`             | Seed for the random input-selection topology (deterministic across runs). |
| `--pool-size`     | `4`                      | `SpiderClient` gRPC connection-pool size.                      |
| `--print-outputs` | off                      | Print each final-layer output + whether it matches the expected payload. |

**Output convention:** per-output inspection (with `--print-outputs`) goes to **stdout**; the
submit line, the per-phase timing breakdown, the ideal-runtime figures, and the success summary go
to **stderr**. So `client ... --print-outputs > outputs.txt` captures just the inspection output.

## How the client works

1. **Build graph** — construct the layered `nn_bench::sleep` `TaskGraph`: layer 0's `width` tasks
   take their 25 inputs from the graph inputs; every inner task's 25 inputs are distinct random
   outputs from the previous layer (seeded by `--seed`). The builder also records the graph's task
   count and depth (the longest dependency chain) for the ideal-runtime step.
2. **Build inputs** — `width * 25` byte vectors of `input_bytes` bytes, one per positional input
   of the layer-0 tasks, each msgpack-encoded into a `TaskInput::ValuePayload`.
3. **Submit → start → poll** — register a per-run resource group, `submit_job`, `start_job`, then
   poll `get_job_state` (every 500 ms) until the job reaches a terminal state.
4. **Verify outputs** — on success, decode each final-layer output and check it equals the fixed
   128-byte payload the task emits, proving every task ran. On failure, fetch and report the job
   error.
5. **Report timing** — print a phase breakdown to stderr: `graph_and_inputs`, `connect_and_resource_group`,
   `submit_and_start`, `spider_execution` (the poll-to-terminal wait — where nearly all the
   wall-clock lives), `post_processing`, plus the `query_processing` / `spider_execution` /
   `post_processing` / `total` rollups.
6. **Report ideal runtime** — print the analytic lower bound for 16, 32, 64, and 128 workers (see
   below).

## The task (`nn_bench::sleep`)

A single TDL task with signature `sleep(ctx, i0..i24) -> Result<Vec<u8>, TdlError>` — 25 `bytes`
positional inputs modeling the neuron's 25 incoming data-flow edges. It sums the input lengths
(only to prove the inputs were delivered), sleeps 10 ms, and returns the fixed 128-byte payload.
Each invocation logs a START and END line to stderr carrying the job/task ids and a nanosecond
timestamp, so per-task start/end times can be recovered from the executor logs.

## Ideal runtime

Because every task costs a fixed 10 ms (a simulated sleep), the workload's ideal makespan is
analytic — no separate baseline run is needed. For a DAG of equal-duration tasks scheduled on `W`
workers, no schedule can beat

```
ideal(W) = max(critical_path, total_work / W)
```

where `total_work = total_tasks * 10 ms` (the perfect-parallelism bound) and `critical_path =
depth * 10 ms` (the longest dependency chain, which must run serially). The client computes this
from the generated graph's task count and depth and prints it for 16, 32, 64, and 128 workers:

```
[timing] == ideal (lower bound):
[timing] ideal 16 workers:            6250.0 ms
[timing] ideal 32 workers:            3125.0 ms
[timing] ideal 64 workers:            1562.5 ms
[timing] ideal 128 workers:            781.2 ms
```

(For the default `--level 10 --width 1000`: 10,000 tasks, depth 10, so `critical_path = 100 ms`
and `total_work = 100 s`.) Compare the measured `[timing] == spider_execution` against the line
matching the stack's worker count — the gap is Spider's scheduling/coordination overhead. As `W`
grows the `total_work / W` term shrinks until the `critical_path` floor dominates (here at ~1000
workers); below that the ideal halves each time `W` doubles.

## Notes

* Prefer a **freshly launched** stack per benchmark: a stack left running can drift into a bad state
  and fail on the next submission.
* The scheduler's `ready_task_capacity` (in `tools/scripts/stack/spider.yaml`) must exceed the total
  number of simultaneously-ready tasks when submitting **concurrent** jobs. Otherwise concurrent
  jobs backpressure the scheduler and become slow and uneven. The default is sized generously for a
  single wide job.
* Per-task START/END timestamps are written by the task itself to stderr, captured by the execution
  manager into `build/spider-run/em-logs/<em_id>-<executor_id>.log`.