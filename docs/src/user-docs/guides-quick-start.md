# Quick start

The guide below briefly describes how to get started with running a task on Spider. At a high-level,
you'll need to:

* Write a task
* Build the task into a shared library
* Write a client to manage the task
* Build the client
* Set up a Spider cluster
* Run the client

The example source code for this guide is in [examples/quick-start].

:::{note}
In the rest of this guide:

1. we specify source file paths relative to `examples/quick-start`.
2. all CMake commands should be run from inside `examples/quick-start`.
:::

# Requirements

In the guide below, you'll need:

* CMake 3.22.1+
* GCC 10+ or Clang 7+
* [Docker] 20.10+
  * If you're not running as root, ensure `docker` can be run
    [without superuser privileges][docker-non-root].

# Writing a task

In Spider, a task is a C++ function that satisfies the following conditions:

* It is a non-member function.
* It takes one or more parameters:
  * The first parameter must be a `TaskContext`.
  * All other parameters must have types that conform to the `Serializable` or `Data` interfaces.
* It returns a value that conforms to the `Serializable` or `Data` interfaces.

:::{note}
You don't immediately need to understand the TaskContext, Serializable, or Data types as we'll
explain them in other guides.
:::

For example, the task in `src/tasks.cpp` computes and returns the sum of two integers:

:::{literalinclude} ../../../examples/quick-start/src/tasks.cpp
:caption: src/tasks.cpp: The example task.
:language: cpp
:lines: 5-12
:lineno-start: 5
:linenos: true
:::

:::{note}
The task is split into a header file and an implementation file so that it can be loaded as a
library in the worker, as we'll see in later sections.
:::

The integer parameters and return value are `Serializable` values.

The `SPIDER_REGISTER_TASK` macro at the bottom of `src/tasks.cpp` is how we inform Spider that a
function should be treated as a task.

# Building the task into a shared library

In order for Spider to run a task, the task needs to be compiled into a shared library that Spider
can load. The example's `CMakeLists.txt` demonstrates how to do this.

To build the shared library, run:

```shell
cmake -S . -B build
cmake --build build --parallel $(nproc) --target tasks
```

# Writing a client to manage the task

To make Spider to run a task, we first need to write a client application. Generally, a client:

1. connects to Spider;
2. submits the task for execution;
3. waits for its completion—whether it succeeds or fails;
4. and then handles the result.

For example, the client in `src/client.cpp` runs the `sum` task from the previous section and
verifies its result:

:::{literalinclude} ../../../examples/quick-start/src/client.cpp
:caption: src/client.cpp: A snippet of the example client.
:language: cpp
:lines: 24-35
:lineno-start: 24
:linenos: true
:::

When we submit a task to Spider, Spider returns a `Job`, which represents a scheduled, running, or
completed task (or `TaskGraph`) in a Spider cluster.

:::{note}
`Job`s and `TaskGraph`s will be explained in another guide.
:::

# Building the client

The client can be compiled like any normal C++ application, except that we need to link it to the
Spider client library and the `tasks` library. The example's `CMakeLists.txt` demonstrates how to do
this.

To build the client executable, run:

```shell
cmake --build build --parallel $(nproc) --target client
```

# Setting up a Spider cluster

Before we can run the client, we need to start a Spider cluster. The simplest Spider cluster
consists of:

* a storage backend;
* a scheduler instance;
* and a worker instance.

## Setting up a storage backend

Spider currently supports using MySQL or MariaDB as a storage backend. In this guide, we'll start
MariaDB in a Docker container:

```shell
docker run \
        --detach \
        --rm \
        --name spider-storage \
        --env MARIADB_USER=spider \
        --env MARIADB_PASSWORD=password \
        --env MARIADB_DATABASE=spider-storage \
        --env MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=true \
        --publish 3306:3306 mariadb:latest
```

:::{warning}
When the container above is stopped, the database will be deleted. In production, you should set up
a database instance with some form of data persistence.
:::

:::{warning}
The container above is using hardcoded default credentials that shouldn't be used in production.
:::

Alternatively, if you have an existing MySQL/MariaDB instance, you can use that as well. Simply
create a database and authorize a user to access it.

## Setting up the scheduler

To build the scheduler, run:

```shell
cmake --build build --parallel $(nproc) --target spider_scheduler
```

To start the scheduler, run:

```shell
build/spider/src/spider/spider_scheduler \
        --storage_url \
        "jdbc:mariadb://localhost:3306/spider-storage?user=spider&password=password" \
        --host "127.0.0.1" \
        --port 6000
```

NOTE:

* If you used a different set of arguments to set up the storage backend, ensure you update the
  `storage_url` argument in the command.
* In production, change the host to the real IP address of the machine running the scheduler.
* If the scheduler fails to bind to port `6000`, change the port in the command and try again.

## Setting up a worker

To build the worker, run:

```shell
cmake --build build --parallel $(nproc) --target spider_worker
```

To start a worker, run:

```shell
build/spider/src/spider/spider_worker \
        --storage_url \
        "jdbc:mariadb://localhost:3306/spider-storage?user=spider&password=password" \
        --host "127.0.0.1" \
        --libs "build/libtasks.so"
```

NOTE:

* If you used a different set of arguments to set up the storage backend, ensure you update the
  `storage_url` argument in the command.
* In production, change the host to the real IP address of the machine running the worker.
* You can specify multiple task libraries to load. The task libraries must be built with linkage
  to the Spider client library. 

:::{tip}
You can start multiple workers to increase the number of concurrent tasks that can be run on the
cluster.
:::

# Running the client

To run the client:

```shell
build/client "jdbc:mariadb://localhost:3306/spider-storage?user=spider&password=password"
```

NOTE:

If you used a different set of arguments to set up the storage backend, ensure you update the
storage backend URL in the command.

# Next steps

In future guides, we'll explain how to write more complex tasks, as well as how to leverage Spider's
support for fault tolerance.

[Docker]: https://docs.docker.com/engine/install/
[docker-non-root]: https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user
[examples/quick-start]: https://github.com/y-scope/spider/tree/main/examples/quick-start
