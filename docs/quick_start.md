# Spider quick start guide

## Architecture of Spider

A Spider cluster is made up of three components:

* __Database__: Spider stores all the states and data in a fault-tolerant database.
* __Scheduler__: Scheduler is responsible for making scheduling decision when a worker ask for a new
  task to run. It also handles garbage collection and failure recovery.
* __Worker__: Worker executes the task it is assigned to. Once it finishes, it updates the task
  output in database and contacts scheduler for a new task.

Users creates a __client__ to run tasks on Spider cluster. It connects to the database to submit new
tasks and get the results. Clients _never_ directly talks to a scheduler or a worker.

## Set up Spider

To get started,

1. Start a database supported by Spider, e.g. MySql.
2. Start a scheduler and connect it to the database by running
   `spider start --scheduler --db <db_url> --port <scheduler_port>`.
3. Start some workers and connect them to the database by running
   `spider start --worker --db <db_url>`. Starting a worker that can run specific tasks needs to
   link to libraries. We'll cover this later.

## Start a client

Client first creates a Spider client driver and connects it to the database. Spider automatically
cleans up the resource in driver's destructor. User can pass in an optional client id. Two drivers
with same client id cannot run at the same time.

```c++
#include <spider/spider.hpp>

auto main(int argc, char **argv) -> int {
    boost::uuids::string_generator gen;
    spider::Driver driver{"db_url", gen(L"01234567-89ab-cdef-0123-456789abcdef")};
}
```

## Create a task

In Spider, a task is a non-member function that takes the first argument a `spider::Context` object.
It can then take any number of arguments which is `Serializable`.

Tasks can return any `Serialiable` value. If a task needs to return more than one result, uses
`std::tuple` and makes sure all elements of the tuple are `Serializable`.

Spider requires user to register the task function by calling `SPIDER_REGISTER_TASK` statically,
which sets up the function internally in Spider library for later user. Spider requires the function
name to be unique in the cluster.

```c++
// Task that sums to integers
auto sum(spider::Context &context, int x, int y) -> int {
    return x + y;
}

// Task that sorts two integers in non-ascending order
auto sort(spider::Context &context, int x, int y) -> std::tuple<int, int> {
    if (x >= y) {
        return { x, y };
    }
    return { y, x };
}

SPIDER_REGISTER_TASK(sum);
SPIDER_REGISTER_TASK(sort);

```

## Run a task

Spider enables user to start a task on the cluster. Simply call `Driver::start` and provide the
arguments of the task. `Driver::start`returns a `spider::Job` object, which represents the running
task. `spider::Job` takes the output type of the task graph as template argument. You can call
`Job::state` to check the state of the running task, and `Job::wait_complete` to block until job
ends and `Job::get_result`. User can send a cancel signal to Spider by calling `Job::cancel`. Client
can get all running jobs submitted by itself by calling `Driver::get_jobs`.

```c++
auto main(int argc, char **argv) -> int {
    // driver initialization skipped
    spider::Job<int> sum_job = driver.run(sum, 2);
    assert(4 == sum_job.get_result());

    spider::Job<std::tuple<int, int>> sort_job = driver.start(4, 3);
    sort_job.wait_complete();
    assert(std::tuple{3, 4} == sort_job.get_result());
}
```

If you try to compile and run the example code directly, you'll find that it fails because Spider
worker does not know which function to run. User need to compile all the tasks into a shared
library, including the call to `SPIDER_REGISTER_TASK`, and start the worker with the library by
running `spider start --worker --db <db_url> --libs [client_libraries]`.

## Group tasks together

In real world, running a single task is too simple to be useful. Spider lets you bind outputs of
tasks as inputs of another task, similar to `std::bind`. The first argument of `spider::bind` is the
child task. The later arguments are either a `spider::Task` or a `spider::TaskGraph`, whose entire
outputs are used as part of the inputs to the child task, or a `Serializable` or
`spider::Data` that is directly used as input. Spider requires that the types of `Task` or
`TaskGraph` outputs or POD type or `spider::Data` matches the input types of child task.

Binding the tasks together forms a dependencies among tasks, which is represented by
`spider::TaskGraph`. `TaskGraph` can be further bound into more complicated `TaskGraph` by serving
as inputs for another task. You can run the task using `Driver::run` in the same way as running a
single task.

```c++
auto square(spider::Context& context, int x) -> int {
    return x * x;
}

auto square_root(spider::Context& context, int x) -> int {
    return sqrt(x);
}
// task registration skipped
auto main(int argc, char **argv) -> auto {
    // driver initialization skipped
    spider::TaskGraph<int(int, int)> sum_of_square = spider::bind(sum, square, square);
    spider::TaskGraph<int(int, int)> rss = spider::bind(square_root, sum_of_square);
    spider::Job<int> job = driver::start(rss, 3, 4);
    job.wait_complete();
    assert(5 == job.get_result());
}
```

## Run task inside task

Static task graph is enough to solve a lot of real work problems, but dynamically run task graphs
on-the-fly could become handy. Running a task graph inside task is the same as running it from a
client.

```c++
auto gcd(spider:Context& context, int x, int y) -> int {
    if (x < y) {
        std::swap(x, y);
    }
    while (x != y) {
        spider::Job<std:tuple<int, int>> job = context.start(gcd_impl, x, y);
        job.wait_complete();
        x = job.get_result().get<0>();
        y = job.get_result().get<1>();
    }
    return x;
}

auto gcd_impl(spider::Context& context, int x, int y) -> std::tuple<int, int> {
    return { x, x % y};
}
```

## Data on external storage

Often simple `Serializable` value are not enough. However, passing large amount of data around is
expensive. Usually these data is stored on disk or a distributed storage system. For example, an ETL
workload usually reads in data from an external storage, writes temporary data on an external
storage, and writes final data into an external storage.

Spider lets user pass the metadata of these data around in `spider::Data` objects. `Data` stores the
value of the metadata information of external data, and provides crucial information to Spider for
correct and efficient scheduling and failure recovery. `Data` stores a list of nodes which has
locality of the external data, and user can specify if locality is a hard requirement, i.e. task can
only run on the nodes in locality list. `Data` can include a `cleanup`function, which will run when
the `Data` object is no longer reference by any task and client. `Data` has a persist flag to
represent that external data is persisted and do not need to be cleaned up.

```c++
struct HdfsFile {
    std::string url;
};

/**
 * In this example, we run a filter and map on the input stored in Hdfs.
 * Filter writes its output into a temporary Hdfs file, which will be cleaned
 * up by Spider when the task graph finishes.
 * Map reads the temporary files and persists the output in Hdfs file.
 */
auto main(int argc, char** argv) -> int {
    // driver initialization skipped
    // Creates a HdfsFile Data to represent the input data stored in Hdfs.
    spider::Data<HdfsFile> input = spider::Data<HdfsFile>::Builder()
        .mark_persist(true)
        .build(HdfsFile { "/path/to/input" });
    spider::Job<spider::Data<HdfsFile>> job = driver::start(
        driver::bind(map, filter),
        input);
    job.wait_complete();
    std::string const output_path = job.get_result().get().url;
    std::cout << "Result is stored in " << output_path << std::endl;
}

/**
 * Runs filer on the input data from Hdfs file and write the output into a
 * temporary Hdfs file for later tasks.
 *
 * @param input input file stored in Hdfs
 * @return temporary file store in Hdfs
 */
auto filter(spider::Data<Hdfsfile> input) -> spider::Data<HdfsFile> {
    // We can use task id as a unique random number.
    std::string const output_path = std::format("/path/%s", context.task_id());
    std::string const input_path = input.get().url;
    // Creates HdfsFile Data before creating the actual file in Hdfs so Spider
    // can clean up the Hdfs file on failure.
    spider::Data<HdfsFile> output = spider::Data<HdfsFile>::Builder()
        .cleanup([](HdfsFile const& file) { delete_hdfs_file(file); })
        .build(HdfsFile { output_path });
    auto file = hdfs_create(output_path);
    // Hdfs allows reading data from any node, but reading from the nodes where
    // file is stored and replicated is faster.
    std::vector<std::string> nodes = hdfs_get_nodes(file);
    output.set_locality(nodes, false); // not hard locality
    
    // Runs the filter
    run_filter(input_path, file);
    
    return output;
}

/**
 * Runs map on the input data from Hdfs file and persists the output into an
 * Hdfs file.
 *
 * @param input input file stored in Hdfs
 * @return persisted output in Hdfs
 */
auto map(spider::Data<HdfsFile> input) -> spider::Data<HdfsFile> {
    // We use hardcoded path for simplicity in this example. You can pass in
    // the path as an input to the task or use task id as random name as in
    // filter.
    std::string const output_path = "/path/to/output";
    std::string const input_path = input.get().url;
    
    spider::Data<HdfsFile> output = spider::Data<HdfsFile>::Builder()
        .cleanup([](HdfaFile const& file) { delete_hdfs_file(file); })
        .build(HdfsFile { output_path });
    
    run_map(input_path, output_path);
    
    // Now that map finishes, the file is persisted on Hdfs as output of job.
    // We need to inform Spider that the file is not persisted and should not
    // be cleaned up.
    output.mark_persist();
    return output;
}

```

## Using key-value store when tasks restart

Spider provides exactly-once semantics in failure recovery. To achieve this, Spider restarts some
tasks after a task fails. Tasks might want to keep some data around after restart. However, all the
`Data` objects created by tasks are cleaned up on restart. Spider provides a key-value store for
the restarted tasks and restarted clients to retrieve values stored by previous run by `insert_kv`
and `get_kv` from `Context` or `Driver`. Note that a task or client can only get the value created
by itself, and the two different tasks can store two different values using the same key.

```c++
auto long_running(spider::Context& context) {
    std::optional<std::string> state_option = context.get_kv("state");
    if (!state_option.has_value()) {
        long_compute_0();
        context.store_kv("state", "0");
    }
    std::string state = context.get_kv("state").value();
    switch (std::stoi(state)) {
        case 0:
            long_compute_1();
            context.store_kv("state", "1") // Keep running after update key-value store
        case 1:
            long_compute_2();
    }
}
```

## Straggler mitigation

`SPIDER_REGISTER_TASK_TIMEOUT` is same as `SPIDER_REGISTER_TASK`, but accepts a second argument as
timeout in milliseconds. If a task instance executes for longer than the specified timeout, Spider
spawns another task instance running the same function. The task instance that finishes first wins.
Other running task instances are cancelled, and associated data is cleaned up.

The new task instance has a different id, and it is the responsibility of the user to avoid any data
race and deduplicate the output if necessary.
