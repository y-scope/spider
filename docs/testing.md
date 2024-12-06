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

## Running tests

You can use the following tasks to run the set of unit tests that's appropriate.

| Task                          | Description                                                       |
|-------------------------------|-------------------------------------------------------------------|
| `test:all`                    | Runs all unit tests.                                              |
| `test:non-storage-unit-tests` | Runs all unit tests which don't require a storage backend to run. |
| `test:storage-unit-tests`     | Runs all unit tests which require a storage backend to run.       |

If any tests show error messages for the connection function below, revisit the
[setup section](#set-up-mysql-as-storage-backend) and verify that `cStorageUrl` was set correctly.

```c++
REQUIRE( storage->connect(spider::test::cStorageUrl).success() )
```

## GitHub unit test workflow

The [unit_tests.yaml][gh-workflow-unit-tests] GitHub workflow runs the unit tests on push,
pull requests, and daily. Currently, it only runs unit tests that don't require a storage backend.

[gh-workflow-unit-tests]: ../.github/workflows/unit-tests.yaml
