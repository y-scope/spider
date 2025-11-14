# spider-py

This project is a Python package that provides access to Spider, a distributed task execution
framework, enabling seamless integration and task management in Python applications.

## Building/Packing

To manually build a package for distribution, follow the steps below.

### Requirements

* [Task] >= 3.40.0
* [uv] >= 0.7.0
* [MariaDB C Connector][mariadb-c-connector] >= 3.3.1

### Build Commands

* Build a Python wheel:

```shell
task build:spider-py
```

The command above will generate both a `.tar.gz` and a `.whl` package in the `build/spider-py`
directory at the Spider project root.

## Testing

Unit tests are divided into two categories: storage and non-storage tests. Non-storage tests do not
require any external services, while storage tests require a MariaDB instance to be available.

### Non-Storage Unit Tests

To run all non-storage unit tests:

```shell
task test:spider-py-non-storage-unit-tests
```

### Setup MariaDB for Storage Unit Tests

To run storage unit tests, we need to create a MariaDB instance first.

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

After the docker container starts, set up the database table manually by using the SQL script
`tools/scripts/storage/init_db.sql` from the project root.

```shell
mysql -h 127.0.0.1 -u spider -ppassword spider-storage < tools/scripts/storage/init_db.sql
```

### Storage Unit Tests

To run all storage unit tests:

```shell
task test:spider-py-storage-unit-tests
```

This requires a running MariaDB instance as described above.

### All Unit Tests

To run all unit tests (both storage and non-storage):

```shell
task test:spider-py-unit-tests
```

This requires a running MariaDB instance as described above.

## Linting

To run all linting checks:

```shell
task lint:spider-py-check
```

To run all linting checks AND automatically fix any fixable issues:

```shell
task lint:spider-py-fix
```

[Task]: https://taskfile.dev
[uv]: https://docs.astral.sh/uv/
[mariadb-c-connector]: https://mariadb.com/docs/connectors/mariadb-connector-c