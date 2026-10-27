# Spider

Spider is a distributed system for executing user-defined tasks, built for low latency, high
throughput, and robust fault tolerance.

A Spider job is a task graph: a directed acyclic graph of tasks connected by typed data flow.
Tasks are plain Rust functions compiled into task definition library (TDL) packages that Spider's
workers load and execute. Spider handles dependency resolution, scheduling, retries, and recovery,
so tasks run as soon as their inputs are ready and worker capacity is available.

```rust
#[task(name = "complex::add")]
pub fn add(_ctx: TaskContext, a: ComplexVec, b: ComplexVec) -> Result<ComplexVec, TdlError> {
    // ...
}
```

## Why Spider?

* **Low latency**: Spider keeps the state of every running job in memory, eliminating database
  queries for dependency resolution and task dispatch. A MariaDB/MySQL backend serves as the
  durable source of truth.
* **Fault tolerance**:
  * Failed tasks are retried up to a per-task limit.
  * Tasks assigned to failed workers are automatically detected and rescheduled.
  * After a crash or restart, unfinished jobs are recovered from the database and resume
    automatically.
* **Isolation**: Tasks run in separate processes, protecting workers from task-level crashes.
* **Multi-tenancy**: Jobs belong to resource groups, and workers can be dedicated to a single
  resource group.
* **Fair scheduling**: Spider uses round-robin scheduling across resource groups and across jobs
  within each group, preventing any single tenant or job from starving the others.
* **Easy deployment**: Deploy a Spider cluster with
  [Docker Compose](tools/deployment/spider-compose) or on Kubernetes with the
  [Helm chart](tools/deployment/spider-helm).

## Docs

> [!WARNING]
> 🚧 The docs are still under construction.
