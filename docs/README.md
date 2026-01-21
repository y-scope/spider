# Docs

This directory contains the files necessary to generate Sphinx-based documentation websites for
this project. There are two documentation sites:

* `wolf` - Wolf documentation
* `huntsman` - Huntsman documentation

Each site has its own directory with:

* `conf` - Configuration files
* `src` - The actual docs

## Requirements

* [Node.js] >= 16 to be able to [view the output](#viewing-the-output)
* Python 3.10 or higher
* [Task] 3.40.0 or higher

## Build commands

* Build the Wolf site incrementally:

  ```shell
  task docs:wolf:site
  ```
  
  * The output of the build will be in `../build/docs/huntsman`.

* Build the Huntsman site incrementally:

  ```shell
  task docs:huntsman:site
  ```

  * The output of the build will be in `../build/docs/huntsman`.

* Clean up all builds:

  ```shell
  task docs:clean
  ```

## Viewing the output

* Serve the Wolf site:

  ```shell
  task docs:wolf:serve
  ```

* Serve the Huntsman site:

  ```shell
  task docs:huntsman:serve
  ```

The commands above will install [http-server] and serve the built docs site; `http-server` will
print the address it binds to (usually http://localhost:8080).

[http-server]: https://www.npmjs.com/package/http-server
[Node.js]: https://nodejs.org/en/download/current
[Task]: https://taskfile.dev/
