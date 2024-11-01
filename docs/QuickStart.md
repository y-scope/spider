# Spider Quick Start Guide

## Set Up Spider
To get started, first start a database supported by Spider, e.g. MySql. Second, start a scheduler and connect it to the database by running `spider start --scheduler --db <db_url> --port <scheduler_port>`. Third, start some workers and connect them to the database by running `spider start --worker --db <db_url>`.

## Start a Client
Client first creates a Spider client driver and connects it to the database. Spider automatically cleans up the resource in driver's destructor, but you can close the driver to release the resource early.
```c++
#include <spider/Spider.hpp>

auto main(int argc, char **argv) -> int {
    spider::Driver driver{};
    driver.connect("db_url");
    
    driver.close();
}
```

## Create a Task
In Spider, a task is a non-member function that takes the first argument a `spider::Context` object. It can then take any number of arguments of POD type.

Task can return any POD type. If a task needs to return more than one result, uses `std::tuple`.

The `Context` object represents the context of a running task. It provides methods to get the task metadata information like task id. It also supports the creating task inside a task. We will cover this later.
```c++
auto sum(spider::Context &context, int x, int y) -> int {
    return x + y;
}

auto sort(spider::Context &context, int x, int y) -> std::tuple<int, int> {
    if (x >= y) {
        return { x, y };
    }
    return { y, x };
}
```

## Run a Task
Spider enables user to run a task on the cluster. First register the functions statically so it is known by Spider. Simply call `Driver::run` and provide the arguments of the task. `Driver::run` returns a `spider::Future` object, which represents the result that will be available in the future. You can call `Future::ready` to check if the value in future is available yet. You can use `Future::get` to block and get the value once it is available. 
```c++
spider::register_task(sum);
spider::register_task(sort);

auto main(int argc, char **argv) -> int {
    // driver initialization skipped
    spider::Future<int> sum_future = driver.run(sum, 2);
    assert(4 == sum_future.get());

    spider::Future<std::tuple<int, int>> sort_future = driver.run(4, 3);
    assert(std::tuple{3, 4} == sort_future.get());
}
```

## Group Tasks Together
In real world, running a single task is too simple to be useful. Spider lets you bind outputs of tasks as inputs of another task, similar to `std::bind`. Binding the  tasks together forms a dependencies among tasks, which is represented by `spider::TaskGraph`. `TaskGraph` can be further bound into more complicated `TaskGraph` by serving as inputs for another task. You can run the task using `Driver::run` in the same way as running a single task.
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

## Run Task inside Task
Static task graph is enough to solve a lot of real work problems, but dynamically add tasks on-the-fly could become handy. As mentioned before, spider allows you to add another task as child of the running task by calling `Context::add_child`.

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

However, it is impossible to get the return value of the task graph from a client. We have a solution by sharing data using key-value store, which will be discussed later. Another solution is to run task or task graph inside a task and wait for its value, just like a client. This solution is closer to the conventional function call semantic.

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

## Data on External Storage
Often simple POD data are not enough. However, passing large amount of data around is expensive. Usually these data is stored on disk or a distributed storage system. For example, an ETL workload usually reads in data from an external storage, writes temporary data on an external storage, and writes final data into an external storage.

Spider lets user pass the metadata of these data around in `spider::Data` objects. `Data` stores the value of the metadata information of external data, and provides crucial information to Spider for correct and efficient scheduling and failure recovery. `Data` stores a list of nodes which has locality of the external data, and user can specify if locality is a hard requirement, i.e. task can only run on the nodes in locality list. `Data` can include a `cleanup`function, which will run when the `Data` object is no longer reference by any task and client. `Data` has a persist flag to represent that external data is persisted and do not need to be cleaned up.

```c++
struct HdfsFile {
    std::string url;
};

auto filter(spider::Data<Hdfsfile> input) -> spider::Data<HdfsFile> {
    std::string const output_path = std::format("/path/%s", context.task_id());
    std::string const input_path = input.get().url;
    // Create HdfsFile Data first in case task fails and Spider can clean up the data.
    spider::Data<HdfsFile> output = spider::Data<HdfsFile>::Builder()
        .cleanup([](HdfsFile const& file) { delete_hdfs_file(file); })
        .build(HdfsFile { output_path });
    auto file = hdfs_create(output_path);
    std::vector<std::string> nodes = hdfs_get_nodes(file);
    output.set_locality(nodes, false); // not hard locality
    
    run_filter(input_path, file);
    
    return output;
}

auto map(spider::Data<HdfsFile> input) -> spider::Data<HdfsFile> {
    std::string const output_path = "/path/to/output";
    std::string const input_path = input.get().url;
    
    spider::Data<HdfsFile> output = spider::Data<HdfsFile>::Builder()
        .cleanup([](HdfaFile const& file) { delete_hdfs_file(file); })
        .build(HdfsFile { output_path });
    
    run_map(input_path, output_path);
    
    // Now that map finishes, the file is persisted on Hdfs as output of job.
    output.mark_persist();
    return output;
}

auto main(int argc, char** argv) -> int {
    spider::Data<HdfsFile> input = spider::Data<HdfsFile>::Builder()
        .mark_persist(true)
        .build(HdfsFile { "/path/to/input" });
    spider::Future<spider::Data<HdfsFile>> future = spider::run(
        spider::bind(map, filter),
        input);
    std::string const output_path = future.get().get().url;
    std::cout << "Result is stored in " << output_path << std::endl;
}
```

## Data as Key-Value Store
`Data` can also be used a a key-value store. User can specify a key when creating the data, and the data can be accessed later by its key. Notice that a task can only access the `Data` created by itself or passed to it. Client can access any data with the key.
Using the key value store, we can solve the dynamic task result problem.

```c++
auto gcd(spider::Context& context, int x, int y, std::string key)
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

## Straggler Mitigation
`Driver::register_task` can take a second argument for timeout milliseconds. If a task executes for longer than the specified timeout, Spider spawns another task instance running the same function. The task that finishes first wins. Other running task instances are cancelled, and associated data is cleaned up.

The new task has a different task id, and it is the responsibility of the user to avoid any data race and deduplicate the output if necessary.

## Note on Worker Setup
The setup section said that we can start a worker by running `spider start --worker --db <db_url>`. This is oversimplified. The worker has to know the function it will run.

When user compiles the client code, an executable and a library are generated. The executable executes the client code as expected. The library contains all the functions registered by user. Worker needs to run with a copy of this library. The actual commands to start a worker is `spider start --worker --db <db_url> --libs [client_libraries]`.