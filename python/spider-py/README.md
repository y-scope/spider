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

To run all unit tests:

```shell
task test:spider-py-unit-tests
```

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