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
cleans up the resource in driver's destructor.

```c++
#include <spider/Spider.hpp>

auto main(int argc, char **argv) -> int {
    spider::Driver driver{"db_url"};
}
```

## Create a task

In Spider, a task is a non-member function that takes the first argument a `spider::Context` object.
It can then take any number of arguments of POD type or `spider::Data` covered
in [later section](#data-on-external-storage).

Task can return any POD type or `spider::Data`. If a task needs to return more than one result, uses
`std::tuple` and makes sure all elements of `std::tuple` are POD or `spider::Data`.

Spider requires user to register the task function using static `spider::register_task`, which
sets up the function internally in Spider library for later user. Spider requires the function name
to be unique in the cluster.

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

spider::register_task(sum);
spider::register_task(sort);

```

## Run a task

Spider enables user to run a task on the cluster. Simply call `Driver::run` and provide the
arguments of the task. `Driver::run`returns a `spider::Future` object, which represents the result
that will be available in the future. You can call `Future::ready` to check if the value in future
is available yet. You can use`Future::get` to block and get the value once it is available.

```c++
auto main(int argc, char **argv) -> int {
    // driver initialization skipped
    spider::Future<int> sum_future = driver.run(sum, 2);
    assert(4 == sum_future.get());

    spider::Future<std::tuple<int, int>> sort_future = driver.run(4, 3);
    assert(std::tuple{3, 4} == sort_future.get());
}
```

If you try to compile and run the example code directly, you'll find that it fails because Spider
worker does not know which function to run. User need to compile all the tasks into a shared
library, including the call to `spider::register_task`, and start the worker with the library by
running `spider start --worker --db <db_url> --libs [client_libraries]`.

## Group tasks together

In real world, running a single task is too simple to be useful. Spider lets you bind outputs of
tasks as inputs of another task, similar to `std::bind`. The first argument of `spider::bind` is the
child task. The later arguments are either a `spider::Task` or a `spider::TaskGraph`, whose entire
outputs are used as part of the inputs to the child task, or a POD or
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
    spider::Future<int> future = driver::run(rss, 3, 4);
    assert(5 == future.get());
}
```

## Run task inside task

Static task graph is enough to solve a lot of real work problems, but dynamically add tasks
on-the-fly could become handy. As mentioned before, spider allows you to add another task as child
of the running task by calling `Context::add_child`.

```c++
auto gcd(spider::Conect& context, int x, int y) -> std::tuple<int, int> {
    if (x == y) {
        std::cout << "gdc is: " << x << std::endl;
        return { x, y };
    }
    if (x > y) {
        context.add_child(gcd);
        return { x % y, y };
    }
    context.add_child(gcd);
    return { x, y % x };
}
```

However, it is impossible to get the return value of the dynamically created tasks from a client. We
have a solution by sharing data using key-value store, which will be discussed
[later](#data-as-key-value-store). Another solution is to run task or task graph inside a task and
wait for its value, just like a client. This solution is closer to the conventional function call
semantic.

```c++
auto gcd(spider:Context& context, int x, int y) -> int {
    if (x < y) {
        std::swap(x, y);
    }
    while (x != y) {
        spider::Future<std:tuple<int, int>> future = context.run(gcd_impl, x, y);
        x = future.get().get().get<0>();
        y = future.get().get().get<1>();
    }
    return x;
}

auto gcd_impl(spider::Context& context, int x, int y) -> std::tuple<int, int> {
    return { x, x % y};
}
```

## Data on external storage

Often simple POD data are not enough. However, passing large amount of data around is expensive.
Usually these data is stored on disk or a distributed storage system. For example, an ETL workload
usually reads in data from an external storage, writes temporary data on an external storage, and
writes final data into an external storage.

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
    // Creates a HdfsFile Data to represent the input data stored in Hdfs.
    spider::Data<HdfsFile> input = spider::Data<HdfsFile>::Builder()
        .mark_persist(true)
        .build(HdfsFile { "/path/to/input" });
    spider::Future<spider::Data<HdfsFile>> future = spider::run(
        spider::bind(map, filter),
        input);
    std::string const output_path = future.get().get().url;
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

## Data as key-value store

`Data` can also be used as a key-value store. User can specify a key when creating the data, and the
data can be accessed later by its key. Notice that a task can only access the `Data` created by
itself or passed to it. Client can access any data with the key.

Using the key value store, we can solve the dynamic task result problem
mentioned [before](#run-task-inside-task).

```c++
auto gcd(spider::Context& context, int x, int y, const char* key)
    -> std::tuple<int, int, std::string> {
    if (x == y) {
        spider::Data<int>.Builder()
            .set_key(key)
            .build(x);
        return { x, y, key };
    }
    if (x > y) {
        context.add_child(gcd);
        return { x % y, y, key };
    }
    context.add_child(gcd);
    return { x, y % x, key };
}

auto main(int argc, char** argv) -> int {
    std::string const key = "random_key";
    driver.run(gcd, 48, 18, key);
    while (!driver.get_data_by_key(key)) {
        int value = driver.get_data_by_key(key).get();
        std::cout << "gcd of " << x << " and " << y << " is " << value << std::endl;
    }
}
```

## Straggler mitigation

`Driver::register_task` can take a second argument for timeout milliseconds. If a task executes for
longer than the specified timeout, Spider spawns another task instance running the same function.
The task that finishes first wins. Other running task instances are cancelled, and associated data
is cleaned up.

The new task has a different task id, and it is the responsibility of the user to avoid any data
race and deduplicate the output if necessary.
