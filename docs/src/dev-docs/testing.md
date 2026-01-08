# Testing

## Running unit tests

You can use the following tasks to run the set of unit tests that's appropriate.

| Task                                    | Description                                                                 |
|-----------------------------------------|-----------------------------------------------------------------------------|
| `test:cpp-unit-tests`                   | Runs all C++ unit tests.                                                    |
| `test:cpp-non-storage-unit-tests`       | Runs all C++ unit tests which don't require a storage backend to run.       |
| `test:cpp-storage-unit-tests`           | Runs all C++ unit tests which require a storage backend to run.             |
| `test:spider-py-unit-tests`             | Runs all spider-py unit tests.                                              |
| `test:spider-py-non-storage-unit-tests` | Runs all spider-py unit tests which don't require a storage backend to run. |
| `test:spider-py-storage-unit-tests`     | Runs all spider-py unit tests which require a storage backend to run.       |

## Running integration tests

You can use the following tasks to run integration tests.

| Task                   | Description                     |
|------------------------|---------------------------------|
| `test:cpp-integration` | Runs all C++ integration tests. |

## GitHub test workflow

The [tests.yaml][gh-workflow-tests] GitHub workflow runs all unit tests and integration tests on
push, pull requests, and daily. 


[gh-workflow-tests]: https://github.com/y-scope/spider/blob/main/.github/workflows/tests.yaml
