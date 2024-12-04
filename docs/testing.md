# Testing

## Set up storage backend

Spider relies on a fault-tolerant storage to store metadata and data. Spider's unit tests also
require this storage backend.

### Set up MySQL as storage backend

1. Start a MySQL database running in background.
2. Create an empty database.
   ```sql
   CREATE DATABASE <db_name>;
   ```
3. Set the password for `root` or create another user with password and grant access to database
   created in step 2.
   ```sql
   ALTER USER 'root'@'localhost' IDENTIFIED BY '<pwd>';
   --- OR create a new user
   CREATE USER '<usr>'@'localhost' IDENTIFIED BY '<pwd>';
   GRANT ALL PRIVILEGES ON <db_name>.* TO '<usr>'@'localhost';
   ```
4. Set the `cStorageUrl` in `tests/storage/StorageTestHelper.hpp` to
   `jdbc:mariadb://localhost:3306/<db_name>?user=<usr>&password=<pwd>`.

## Build and run unit tests

To build and run the unit tests, run the following commands in project root directory.

```shell
cmake -S . -B build
cmake --build build --target unitTest --parallel
./build/tests/unitTest
```

If the tests show error messages for connection functions below,
revisit [Setup storage backend](#setup-storage-backend) section and double check if `cStorageUrl` is
set correctly.

```c++
REQUIRE( storage->connect(spider::test::cStorageUrl).success() )
```

## Test tasks

Three unit-test related tasks are added to taskfile.

* `test:non-storage-unit-tests` runs all unit tests without `[storage]` tag, i.e. do not need a storage to run.
* `test:storage-unit-tests` runs all unit tests that has `[storage]` tag.
* `test:all` runs all unit tests.

All unit-test related tasks build `spider_task_executor`, `unitTest` and `worker_test`, which are necessary to run unit
tests.

## GitHub test workflow

A GitHub workflow `unit_tests.yaml` is set up to run unit test on push, pull request and every day. Currently, it only
runs unit tests without storage requirement.