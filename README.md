# Docs

You can find our docs [online][spider-docs]. 

# Contributing
Follow the steps below to develop and contribute to the project.

## Requirements
* Python 3.10 or higher
* [Task] 3.40.0 or higher
* [uv] 0.7.10 or higher

## Set up
Run dependency installation task:
```shell
task deps:lib_install
```

Set up the config files for our C++ linting tools:
```shell
task lint:cpp-configs
```

## Adding files
Certain file types need to be added to our linting rules manually:

* **CMake**. If adding a CMake file, add it (or its parent directory) as an argument to the
  `gersemi` command in [lint-tasks.yaml](lint-tasks.yaml).
  * If adding a directory, the file must be named `CMakeLists.txt` or use the `.cmake` extension.
* **YAML**. If adding a YAML file (regardless of its extension), add it as an argument to the
  `yamllint` command in [lint-tasks.yaml](lint-tasks.yaml).

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
| `lint:cmake-check`      | Runs the CMake linters.                                  |
| `lint:cmake-fix`        | Runs the CMake linters and fixes any violations.         |
| `lint:cpp-check`        | Runs the C++ linters (formatters and static analyzers).  |
| `lint:cpp-fix`          | Runs the C++ linters and fixes some violations.          |
| `lint:cpp-format-check` | Runs the C++ formatters.                                 |
| `lint:cpp-format-fix`   | Runs the C++ formatters and fixes some violations.       |
| `lint:cpp-static-check` | Runs the C++ static analyzers.                           |
| `lint:cpp-static-fix`   | Runs the C++ static analyzers and fixes some violations. |
| `lint:yml-check`        | Runs the YAML linters.                                   |
| `lint:yml-fix`          | Runs the YAML linters and fixes some violations.         |

[spider-docs]: https://docs.yscope.com/spider/main/
[Task]: https://taskfile.dev
[uv]: https://docs.astral.sh/uv/
