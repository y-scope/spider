# Storage configuration

The storage service is Spider's system of record. It owns the only direct connection to the MariaDB
database, together with the in-memory caches and queues that hold live job state; every other
component reaches that state through the storage gRPC API. The service reads a single YAML file,
passed to the `spider-storage` binary with `--config` and mounted at `/etc/spider/storage.yaml` by
both bundled deployments.

See [Configuration][configuration] for how to apply an override in each deployment type. Every value
below names the [Helm chart value][helm-values] and the [Docker Compose environment
variable][env-example] that set it, or `Not supported` where a deployment offers no way to change
it. Apart from `host` and `port`, which are top-level keys, each name on this page is relative to
the config file's `runtime` section.

:::{note}
Storage doesn't reject unknown config keys, but only keys that have a built-in default survive a
typo. Misspelling one of those—anything under `inbound_queue`, `task_instance_pool`, or
`job_cache_gc`—is silently ignored and leaves the setting at its default; misspelling a required
key, such as the top-level `host` and `port` or `db.name` and `db.max_connections`, aborts startup
with a `missing field` error.
:::

## General

Process-level settings, read once at startup before the storage runtime is built: the address the
gRPC server binds to and how verbosely the service logs.

:::{confval} host
:Helm value: Not supported
:Docker Compose env var: Not supported
:type: `string` (IP address)
:default: `0.0.0.0`

The IP address the storage gRPC server binds to. It is parsed as an IP address, so it must be a
literal IPv4 or IPv6 address rather than a DNS name, and it is combined with `port` to form the
listen address; a bind failure aborts startup. Both deployments hardcode `0.0.0.0` into the config
file they render and expose no override, so changing it means editing the Helm template or the
Compose config template. Under Docker Compose, the host interface that the port is published on is a
separate knob, `SPIDER_STORAGE_PUBLISHED_IP` (default `127.0.0.1`).
:::

:::{confval} port
:Helm value: `spiderConfig.storage.port`
:Docker Compose env var: `SPIDER_STORAGE_PORT`
:type: `int` (0-65535)
:default: `50051`

The TCP port the storage gRPC server listens on; every storage service is served on this one port.
It isn't a performance knob: it only has to be free on the host and to match the storage endpoint
that the scheduler and the execution managers are configured with. Both deployments render the
clients' endpoints from this same value, so change it in one place. Under Docker Compose the
container-internal port and the host-published port are separate variables:
`SPIDER_STORAGE_PUBLISHED_PORT` (default `50051`) selects the host-side port, while the container
healthcheck probes the internal one.
:::

:::{confval} RUST_LOG
:Helm value: `spiderConfig.storage.log_level`
:Docker Compose env var: `SPIDER_STORAGE_LOG_LEVEL`
:type: `string` (tracing filter directive)
:default: `INFO`

The log filter for the storage process. This is an environment variable rather than a config-file
key: the storage config has no log-level field, and logging is initialized from `RUST_LOG`. When
`RUST_LOG` is unset the filter falls back to `ERROR`; both deployments set `INFO` explicitly. Any
filter syntax works, so per-target directives such as `spider_storage=debug,info` are accepted, not
just a bare level. Raising the level to `DEBUG` turns on per-request traces such as the
inbound-queue polls, which helps when diagnosing scheduling stalls. Logs are emitted as JSON to
stderr through a lossless writer, so a very verbose level combined with a slow stderr consumer
applies back-pressure to the async runtime instead of dropping events.
:::

## Database

How storage reaches the MariaDB instance that holds Spider's durable state—resource groups, jobs,
sessions, execution managers, and schedulers—and how many connections it keeps open to it. The
`db` section is mandatory and none of its fields have built-in defaults, so both deployments always
render them. Storage issues its table-creation DDL on every startup, so the schema must already
exist and the configured user must hold DDL rights on it.

:::{confval} db.host
:Helm value: `spiderConfig.database.host`
:Docker Compose env var: Not supported
:type: `string` (hostname or IP address)
:default: `127.0.0.1` in Helm; `spider-database` in Docker Compose

The hostname or IP address of the MariaDB server. It is resolved when the connection pool is created
at startup; if the database is unreachable, the process exits before the gRPC server starts. In Helm
this value only takes effect for an external database: while `"database"` is listed in
`spiderConfig.bundled` (the default), the chart substitutes the bundled database Service's name
(`spider-database` for a release named `spider`) and ignores `spiderConfig.database.host`. Docker
Compose hardcodes the bundled service name `spider-database` and has no external-database mode at
all.
:::

:::{confval} db.port
:Helm value: `spiderConfig.database.port`
:Docker Compose env var: `SPIDER_DATABASE_PORT`
:type: `int` (0-65535)
:default: `3306`

The TCP port of the MariaDB server. As with `db.host`, Helm honours this value only for an external
database: while the database is bundled, the chart emits a literal `3306`, which is also the bundled
database's container port, and ignores `spiderConfig.database.port`. Under Docker Compose,
`SPIDER_DATABASE_PORT` is passed both to storage's config file and to MariaDB itself, so the two
sides stay consistent.
:::

:::{confval} db.name
:Helm value: `spiderConfig.database.name`
:Docker Compose env var: `SPIDER_DATABASE_NAME`
:type: `string`
:default: `spider-db`

The schema that storage connects to. Both deployments pass the same value to the bundled MariaDB as
the schema to create on first boot. Storage creates its tables inside this schema on every startup,
so the configured user needs DDL rights on it. Two storage deployments pointed at one schema share
all durable state, so give independent clusters independent schema names.
:::

:::{confval} db.max_connections
:Helm value: `spiderConfig.database.max_connections`
:Docker Compose env var: `SPIDER_STORAGE_DB_MAX_CONNECTIONS`
:type: `int`
:default: `64`

The upper bound on storage's MariaDB connection pool. Every database-touching operation borrows a
connection from it: each gRPC handler that reads or writes job, resource-group, or session state,
each execution-manager heartbeat, and the task instance pool's per-cycle query for dead execution
managers. The pool's other options are left at their defaults—connections open lazily, and an
acquire that finds the pool exhausted waits 30 seconds and then fails rather than opening more.
Raising the bound buys request concurrency at the cost of memory and threads on the database side;
keep the total across all storage replicas below the server's own connection limit. Lowering it
queues work at acquire time, which shows up as added latency on every storage RPC rather than only
on the slow ones. Size it from the peak concurrent database work: the heartbeat rate—the number
of execution managers divided by their storage heartbeat interval—multiplied by how long a
heartbeat holds a connection, plus the job-submission and task-lifecycle RPCs in flight at once,
plus one for the GC cycle.
:::

:::{confval} db.credentials.username
:Helm value: `spiderConfig.database.username`
:Docker Compose env var: `SPIDER_STORAGE_DB_USERNAME`
:type: `string`
:default: `spider-user`

The MariaDB user that storage authenticates as. Neither deployment writes this key into the config
file: when the `db.credentials` mapping is absent, storage reads the `SPIDER_STORAGE_DB_USERNAME`
environment variable instead, and config loading fails naming that variable if it is unset. Helm
renders the value into the release's database secret and injects it into the storage container from
there; Docker Compose passes the variable straight through. Supplying `db.credentials` in the config
file overrides the environment entirely, and both `username` and `password` are then mandatory.
:::

:::{confval} db.credentials.password
:Helm value: `spiderConfig.database.password`
:Docker Compose env var: `SPIDER_STORAGE_DB_PASSWORD`
:type: `string` (secret)
:default: `spider-password`

The password for `db.credentials.username`, supplied through the same mechanism. Storage holds it as
a secret value, so it is redacted from debug output and never serialized back out. Prefer the
environment-variable form that both deployments use: putting the password in the config file writes
it to disk in plaintext. Override the shipped default before exposing a deployment to anything
beyond a local test—under Docker Compose the value is also visible through `docker inspect`.
:::

## Inbound queue

Three independent bounded in-memory lanes—one for regular tasks, one for commit tasks, and one
for cleanup tasks—in which storage buffers work that has become schedulable until a scheduler
drains it with a poll RPC. Each capacity is a hard bound on how much ready work storage will hold
before its producers block: sends are awaited, so a full lane stalls job start, task-completion
handling, and the task instance pool's re-enqueue path. All three values are optional and must be
greater than zero, and the section may be omitted entirely. Both deployments override the built-in
defaults for every lane.

:::{confval} inbound_queue.task_capacity
:Helm value: `spiderConfig.storage.runtime.inbound_queue.task_capacity`
:Docker Compose env var: `SPIDER_STORAGE_INBOUND_QUEUE_TASK_CAPACITY`
:type: `int` (> 0)
:default: `1048576`

The capacity of the regular-task lane: the maximum number of ready-task notifications storage
buffers for schedulers to poll. One entry is enqueued per ready task, so a job that fans out to N
parallel tasks pushes N entries. This is the largest single memory knob in the service—the lane
is a ring pre-allocated when the channel is created, at 32 bytes per slot, so the default reserves
roughly 32 MiB at startup regardless of load. Set it above the peak number of simultaneously ready
tasks across all running jobs; that peak is also what a storage restart or a scheduler reconnect
resends in a single burst. Lowering it saves memory but converts such a burst, or a gap in scheduler
polling, into blocked storage RPCs. The built-in default is `65536`, which both deployments raise to
`1048576`. It is independent of the scheduler's own ready-task capacity.
:::

:::{confval} inbound_queue.commit_capacity
:Helm value: `spiderConfig.storage.runtime.inbound_queue.commit_capacity`
:Docker Compose env var: `SPIDER_STORAGE_INBOUND_QUEUE_COMMIT_CAPACITY`
:type: `int` (> 0)
:default: `256`

The capacity of the commit-task lane, which carries one entry per job that has become commit-ready.
Because a job normally contributes one entry at a time (a storage restart or a scheduler reconnect
can re-send a duplicate for a job whose entry is still queued), size it by the number of jobs that
can be commit-ready simultaneously rather than by task count—which is why it is orders of
magnitude smaller than the task lane. Memory is negligible at 24 bytes per pre-allocated slot. Note
that the built-in default is `1024`, which both deployments lower to `256`.
:::

:::{confval} inbound_queue.cleanup_capacity
:Helm value: `spiderConfig.storage.runtime.inbound_queue.cleanup_capacity`
:Docker Compose env var: `SPIDER_STORAGE_INBOUND_QUEUE_CLEANUP_CAPACITY`
:type: `int` (> 0)
:default: `256`

The capacity of the cleanup-task lane, which carries one entry per job that has become
cleanup-ready. Size it like the commit lane, by the number of jobs that can be cleanup-ready at
once; memory is likewise negligible. Cleanup entries are produced both by the normal completion
path and when a job is cancelled or fails, so under-sizing this lane can stall job teardown. As with
the commit lane, the built-in default is `1024` and both deployments lower it to `256`.
:::

## Task instance pool

A single coroutine that tracks every in-flight task instance. On a fixed interval it detects
execution managers that have stopped heartbeating and task instances whose soft timeout has elapsed,
and re-enqueues the affected tasks onto the inbound queue so they can be scheduled again. All three
values are optional and must be greater than zero, and the section may be omitted entirely; both
deployments set the built-in defaults explicitly.

:::{confval} task_instance_pool.execution_manager_stale_cutoff_sec
:Helm value: `spiderConfig.storage.runtime.task_instance_pool.execution_manager_stale_cutoff_sec`
:Docker Compose env var: `SPIDER_STORAGE_TASK_INSTANCE_POOL_EXECUTION_MANAGER_STALE_CUTOFF_SEC`
:type: `int` (seconds, > 0)
:default: `60`

The liveness window for execution managers. Once per GC cycle, storage marks every execution manager
still recorded as alive whose last heartbeat is older than this cutoff as dead, force-removes its
in-flight task instances, and re-enqueues their tasks. Lowering it recovers a crashed or partitioned
worker's tasks sooner, but makes it likelier that a slow or briefly unreachable worker is declared
dead—which duplicates its in-flight tasks, since the original keeps running until it is
force-removed, and causes its later task-instance registrations to be rejected and re-enqueued. The
transition to dead is recorded in the database, so it isn't undone when the worker comes back.
Raising it makes false positives less likely but leaves orphaned tasks stuck for up to the cutoff
plus one `task_instance_pool.gc_interval_sec` before anything re-runs them. Keep it a comfortable
multiple of the execution managers' storage heartbeat interval, which defaults to one second, so
that ordinary jitter can't trip it. It is independent of the scheduler's own dead-execution-manager
cutoff, which defaults to `30`.
:::

:::{confval} task_instance_pool.gc_interval_sec
:Helm value: `spiderConfig.storage.runtime.task_instance_pool.gc_interval_sec`
:Docker Compose env var: `SPIDER_STORAGE_TASK_INSTANCE_POOL_GC_INTERVAL_SEC`
:type: `int` (seconds, > 0)
:default: `30`

The tick period of the pool's GC cycle. Each cycle issues one dead-execution-manager query and then
scans every tracked instance, dropping those whose task already reached a terminal state,
force-removing those belonging to dead execution managers, and re-enqueuing those whose soft-timeout
deadline has passed. It therefore sets the resolution of every recovery behaviour in the pool: a
soft timeout is acted on up to one interval late, and dead-worker recovery takes up to
`task_instance_pool.execution_manager_stale_cutoff_sec` plus one interval. Lowering it speeds up
recovery and keeps the set of tracked instances smaller, at the cost of one database round trip per
tick and more time spent scanning; because the pool is a single coroutine, time inside a cycle is
time it isn't draining registrations. Setting it far below the soft timeouts your tasks use gains
nothing, since the extra ticks only re-scan. It is distinct from `job_cache_gc.gc_interval_sec`,
which merely happens to share the same default.
:::

:::{confval} task_instance_pool.message_channel_capacity
:Helm value: `spiderConfig.storage.runtime.task_instance_pool.message_channel_capacity`
:Docker Compose env var: `SPIDER_STORAGE_TASK_INSTANCE_POOL_MESSAGE_CHANNEL_CAPACITY`
:type: `int` (> 0)
:default: `128`

The capacity of the bounded channel that carries task-instance registrations to the pool coroutine.
Every task instance handed to an execution manager is registered through it, and the send is awaited
while the job's control block is locked, so a full channel blocks the registration RPC and the
worker waiting to start the task. The coroutine can be unavailable for a while—mid-GC-cycle, or
blocked on a full inbound-queue lane—so raise this when task dispatch is bursty relative to the
GC interval and you would rather buffer the burst than stall dispatch. Lowering it tightens
back-pressure, pinning dispatch throughput to the coroutine's drain rate. Unlike the inbound queue
lanes, nothing is reserved up front here: the cost is per queued message.
:::

## Job cache garbage collection

A background actor that evicts jobs from storage's in-memory job cache once they have been in a
terminal state for a grace period, trading memory for the ability to answer job-state and job-output
queries without a database round trip. Both values are optional and must be greater than zero, and
the section may be omitted entirely; both deployments set the built-in defaults explicitly.

:::{confval} job_cache_gc.terminated_job_retention_sec
:Helm value: `spiderConfig.storage.runtime.job_cache_gc.terminated_job_retention_sec`
:Docker Compose env var: `SPIDER_STORAGE_JOB_CACHE_TERMINATED_JOB_RETENTION_SEC`
:type: `int` (seconds, > 0)
:default: `300`

How long a job's control block stays in the in-memory job cache after the job reaches a terminal
state. While the job is cached, job-state and job-output lookups are served from memory; once it is
evicted, the same calls fall back to the database, adding a round trip and consuming a pooled
connection per call. Raise it when clients typically fetch results well after completion and you
would rather spend memory than database load—the cost is that every retained job keeps its full
control block, including its task graph and task outputs, resident. Lower it to reclaim memory
sooner in deployments with high job churn. The effective delay is this value plus up to one
`job_cache_gc.gc_interval_sec`, because expiry is only evaluated on a tick.
:::

:::{confval} job_cache_gc.gc_interval_sec
:Helm value: `spiderConfig.storage.runtime.job_cache_gc.gc_interval_sec`
:Docker Compose env var: `SPIDER_STORAGE_JOB_CACHE_GC_INTERVAL_SEC`
:type: `int` (seconds, > 0)
:default: `30`

The tick period of the job-cache GC actor. On each tick it walks its retention queue from the front,
stops at the first entry that hasn't yet expired, and removes all expired jobs in one batch. This is
the granularity of eviction rather than the retention itself: a terminated job is dropped somewhere
between `job_cache_gc.terminated_job_retention_sec` and that value plus one interval after it
terminates. Lowering it tightens that window and returns memory sooner, and the per-tick cost is
small and involves no database I/O, since the scan short-circuits at the first unexpired entry and
the batch is evicted under a single lock. The actor's input queue is unbounded and fire-and-forget,
so a slow tick never applies back-pressure to the RPC path that terminates a job; it only delays
reclamation.
:::

[configuration]: index.md
[env-example]: https://github.com/y-scope/spider/blob/DOCS_VAR_SPIDER_GIT_REF/tools/deployment/spider-compose/.env.example
[helm-values]: https://github.com/y-scope/spider/blob/DOCS_VAR_SPIDER_GIT_REF/tools/deployment/spider-helm/values.yaml
