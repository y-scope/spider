# Quick start

Spider is a distributed system for executing user-defined tasks. It is designed to achieve low
latency, high throughput, and robust fault tolerance.

The guide below briefly describes how to get started with running a task on Spider. At a high-level,
you'll need to:

* Write a task
* Build the task into a shared library
* Write a client to manage the task
* Build the client
* Set up a Spider cluster
* Run the client

> [!NOTE]
> Each code example below is prefixed with a suggested file path that we then use when compiling.
> If you choose different file paths, ensure you update the compilation commands to match.

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

> [!NOTE]
> You don't immediately need to understand the TaskContext, Serializable, or Data types as we'll
> explain them in other guides.

For example, the task below computes and returns the sum of two integers.

> [!NOTE]
> The task is split into a header file and an implementation file so that it can be loaded as a
> library in the worker, as we'll see in later sections.

`src/tasks.hpp`:

```c++
#include <spider/client/spider.hpp>

// Task function prototype
/**
 * @param context
 * @param x
 * @param y
 * @return The sum of x and y.
 */
auto sum(spider::TaskContext& context, int x, int y) -> int;

```

`src/tasks.cpp`:

```c++
#include "tasks.hpp"

#include <spider/client/spider.hpp>

// Task function implementation
auto sum(spider::TaskContext& context, int x, int y) -> int {
    return x + y;
}

// Register the task with Spider
SPIDER_REGISTER_TASK(sum);

```

The integer parameters and return value are `Serializable` values.
The `SPIDER_REGISTER_TASK` macro at the bottom of `src/tasks.cpp` is how we inform Spider that a
function should be treated as a task.

# Building the task into a shared library

In order for Spider to run a task, the task needs to be compiled into a shared library that Spider
can load. To do so, first, copy the Spider project directory into the current directory to create
the following directory structure:

* `spider/`
* `src/`
  * `tasks.cpp`
  * `tasks.hpp`

Then add the following `CMakeLists.txt` to the same directory.

`CMakelists.txt`:

```cmake
cmake_minimum_required(VERSION 3.22.1)
project(spider_example)

# Add the Spider library
add_subdirectory(spider)

# Add the task library
add_library(tasks SHARED src/tasks.cpp src/tasks.hpp)

# Link the Spider library to the task library
target_link_libraries(tasks PRIVATE spider::spider)
```

To build the shared library, run the following from the root of the project:

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

For example, the client below runs the `sum` task from the previous section and verifies its result.

`src/client.cpp`:

```c++
#include <iostream>
#include <string>

#include <spider/client/spider.hpp>

#include "tasks.hpp"

auto main(int argc, char const* argv[]) -> int {
    // Parse the storage backend URL from the command line arguments
    if (argc < 2) {
        std::cerr << "Usage: ./client <storage-backend-url>" << '\n';
        return 1;
    }
    std::string storage_url{argv[1]};
    if (storage_url.empty()) {
        std::cerr << "storage-backend-url cannot be empty." << '\n';
    }

    // Create a driver that connects to the Spider cluster
    spider::Driver driver{storage_url};
    
    // Submit the task for execution
    int x = 2;
    int y = 3;
    spider::Job<int> job = driver.start(sum, x, y);
    
    // Wait for the job to complete
    job.wait_complete();
    
    // Handle the job's success/failure
    auto job_status = job.get_status();
    switch (job_status) {
        case JobStatus::Succeeded: {
            auto result = job_status.get_result();
            int expected = x + y;
            if (expected == result) {
                    return 0;
                } else {
                    std::cerr << "`sum` returned unexpected result. Expected: " << expected
                            << ". Actual: " << result << '\n';
                return 1;
            }
        }
        case JobStatus::Failed:
            std::pair<std::string, std::string> error_and_fn_name = job.get_error();
            std::cerr << "Job failed in function " << error_and_fn_name.second << " - "
                   << error_and_fn_name.first << '\n';
            return 1;
        default:
            std::cerr << "Job is in unexpected state - " << job_status << '\n';
            return 1;
    }
}

```

When we submit a task to Spider, Spider returns a `Job`, which represents a scheduled, running, or
completed task (or `TaskGraph`) in a Spider cluster.

> [!NOTE]
> `Job`s and `TaskGraph`s will be explained in another guide.

# Building the client

The client can be compiled like any normal C++ application, except that we need to link it to the
Spider client library. To do so, add the following to `CMakeLists.txt`:

```cmake
# Add the client
add_executable(client src/client.cpp)

# Link the spider library to the client
target_link_libraries(client PRIVATE spider::spider)
```

To build the client executable, run:

```shell
cmake -S . -B build
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

> [!WARNING]
> When the container above is stopped, the database will be deleted. In production, you should set
> up a database instance with some form of data persistence.

> [!WARNING]
> The container above is using hardcoded default credentials that shouldn't be used in production.

Alternatively, if you have an existing MySQL/MariaDB instance, you can use that as well. Simply
create a database and authorize a user to access it.

## Setting up the scheduler

To build the scheduler, run:

```shell
cmake -S spider -B spider/build
cmake --build spider/build --parallel $(nproc) --target spider_scheduler
```

To start the scheduler, run:

```shell
spider/build/src/spider/spider_scheduler \
        --storage_url \
        "jdbc:mariadb://localhost:3306/spider-storage?user=spider&password=password" \
        --port 6000
```

NOTE:

* If you used a different set of arguments to set up the storage backend, ensure you update the
  `storage_url` argument in the command.
* If the scheduler fails to bind to port `6000`, change the port in the command and try again.

## Setting up a worker

To build the worker, run:

```shell
cmake -S spider -B build
cmake --build spider/build --parallel $(nproc) --target spider_worker
```

To start a worker, run:

```shell
spider/build/src/spider/spider_worker \
        --storage_url \
        "jdbc:mariadb://localhost:3306/spider-storage?user=spider&password=password" \
        --port 6000
```

NOTE:

If you used a different set of arguments to set up the storage backend, ensure you update the
`storage_url` argument in the command.

> [!TIP]
> You can start multiple workers to increase the number of concurrent tasks that can be run on the
> cluster.

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
