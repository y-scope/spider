# Architecture

## Spider Architecture
`Spider` consists of several components that work together to provide a scalable, low-latency and
fault-tolerant distributed task execution system.

```{image} ./arch.png
  :width: 80%
  :align: center
  :alt: Spider Architecture
```

### Storage
`Spdier` relies on a fault-tolerant and ACID storage, e.g. MariaDB, to persist all the states of the
system.
The storage stores the following information:
- Tasks metadata, including:
  - Task ID
  - Task inputs/outputs type and values
  - Task status
- Job metadata, including
  - Job ID
  - Task graph
  - Job status
- Data objects, including:
  - Data object ID
  - Data object type
  - Data object value
  - References from tasks and clients
- Client/Scheduler/Worker metadata, including:
  - Client ID
  - Scheduler ID
  - Worker ID
  - Heartbeat timestamps

### Scheduler
Scheduler is responsible for:
- Allocating tasks to idle workers on their request
- Failure detection and recovery
- Garbage collection
- Straggler detection and task replication
For now `Spider` only supports a single scheduler, and we plan to support multiple schedulers if it
becomes the bottleneck of the system.

### Worker
A worker executes tasks allocated by the scheduler. It runs the following steps in loop:
1. Request a task from the scheduler
2. Fetch task inputs from the storage
3. Spawn a process to execute the task
4. Store task outputs in the storage and update task and job states
Each worker only executes one task at a time.

### Client
Client communicates only with the storage to submit jobs and query job status and fetch job
results.

## Data Abstraction
`Spider` provides a simple data abstraction for task inputs and outputs, which encapsulates the
- locality of the data, i.e. the addresses of the data
- checkpointed or not, i.e. whether the data is persisted
This abstraction allows `Spider` to support:
- locality-aware task scheduling
- fine-grained failure recovery
- garbage collection in background

## Fault Tolerance
`Spider` is designed to be fault-tolerant. The system can recover from failures of a scheduler or a
worker.

Schedulers, workers and clients send periodic heartbeats to the storage to indicate their liveness.
If a scheduler fails, the host can restart a new scheduler instance and fetch the latest state from
the storage.
If a worker fails while executing a task, the scheduler will detect the failure and perform
recovery of the job.
- Identify all the failed tasks within the job
- Compute the minimum subgraph that contains the fail tasks where all inputs to the subgraph are
  available
- Invalidate all the tasks in the subgraph, set the tasks on the input boundary as ready and the
  rest as waiting
