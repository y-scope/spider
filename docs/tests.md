# Tests

## Setup storage backend

Spider relies on a fault-tolerant storage to store metadata and data. Unit tests need the storage
backend available to run the unit tests.

### Setup Mysql as storage backend

1. Start a Mysql database running in background.
2. Set the password for `root` or any create another user with password
3. Create an empty database.
4. Set the `cStorageUrl` in `tests/storage/StorageTestHelper.hpp` to
   `jdbc:mariadb://localhost:3306/<db_name>?user=<usr>&password=<pwd>`

## Build and run unit tests

To build the unit tests, run the following commands in project root directory.

```shell
cmake -S . -B build
cmake --build build --target unitTest --parallel
./build/tests/unitTest
```

If the tests show error message for connection function below, revisit
the [Setup storage backend](#setup-storage-backend) and double check if `cStroageUrl` is correctly
set.

```c++
REQUIRE( storage->connect(spider::test::cStorageUrl).success() )
```