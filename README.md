# Contributing
Follow the steps below to develop and contribute to the project.

## Requirements
* Python 3
* [Task] 3.38.0 or higher

## Set up
Initialize and update submodules:
```shell
git submodule update --init --recursive
```

## Linting
Before submitting a pull request, ensure you’ve run the linting commands below and either fixed any
violations or suppressed the warning.

To run all linting checks:
```shell
task lint:check
```

To run all linting checks AND automatically fix any fixable issues:
```shell
task lint:fix
```

### Running specific linters
The commands above run all linting checks, but for performance you may want to run a subset (e.g.,
if you only changed C++ files, you don't need to run the YAML linting checks) using one of the tasks
in the table below.

| Task                    | Description                                              |
|-------------------------|----------------------------------------------------------|
| `lint:yml-check`        | Runs the YAML linters.                                   |
| `lint:yml-fix`          | Runs the YAML linters and fixes some violations.         |

[Task]: https://taskfile.dev
