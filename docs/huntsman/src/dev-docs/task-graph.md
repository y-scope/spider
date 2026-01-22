# Task graph

In Spider, a task graph serves as the underlying representation of a job. It is a directed acyclic
graph (DAG) that captures a collection of tasks and the dependency relationships among them.

This document specifies the design and semantics of the task graph.

## Specification

### Task

A task is a vertex in the task graph. Each task contains metadata required for execution, including:

* TDL package name: The identifier of the TDL package that contains the task function
  implementation.
* Task function name: The name of the function that implements the task's logic.
* Other metadata such as maximum number of retries allowed.

Tasks are classified based on their position in the graph:

* Input task: A task with no parent tasks. It serves as a starting point for execution.
* Output task: A task with no child tasks. It represents a terminal point in the execution.
* Intermediate task: A task that has both parent and child tasks.

### Task inputs

Each task defines a finite, ordered list of task inputs.

* Task inputs are positional and typed.
* Each input position expects exactly one instance of the declared type.

### Task outputs

Each task defines a finite, ordered list of task outputs.

* Task outputs are positional and typed.
* Each output position produces at most one instance of the declared type upon successful task
  execution.

### Task dependencies

There are two types of dependencies between tasks in the task graph: data flow dependencies and
control flow dependencies.

#### Data and data-flow dependencies

A task data-flow dependency represents a data dependency between tasks in a task graph.

Conceptually, the task graph maintains a set of data objects, each of which represents the flow of a
single typed value from one source to one or more destinations.

##### The endpoints of a data object

The source of a data object is exactly one of the following:

* An external job input provided by the job creator, or
* An output of a task in the task graph.

The destination(s) of a data object are zero or more of the following:

* Inputs of tasks in the task graph, or
* The job output.

A data object **may** have multiple destinations, enabling fan-out from a single source.

##### Task-level data-flow dependencies

Within the task graph, a data object implies one or more task data-flow dependencies.

A task data-flow dependency chains a task output of a parent task (the data source) to a task input
of a child task (the data destination).

The type of the task output and the type of the task input **must** match exactly.

**Constraints on task inputs**

* Input tasks:
  * All task inputs must be provided at job creation time.
  * Each task input corresponds to a data object whose source is an external job input.
  * Input tasks must not depend on the output of any other task.
* Non-input tasks:
  * Every task input must be chained to exactly one output of another task.
  * Each task input corresponds to a data object whose source is a task output.

**Constraints on task outputs**

* A task output may be chained to zero or more task inputs.
  * In this case, the task output serves as the source of a data object consumed by one or more
    downstream task inputs.
* A task output may be dangling.
  * A dangling output is the source of a data object with no task-level destinations and may
    optionally be designated as a job output.

#### Control-flow dependencies

A task control-flow dependency represents an execution ordering constraint between tasks and is
derived from task data-flow dependencies.

* If there exists a task data-flow dependency from an output of task **A** to an input of task
  **B**, then:
  * **A** is a *parent* of **B**, and
  * **B** is a *child* of **A**.
* The set of all parent–child relationships defines the **directed edges** of the task graph.

Control-flow dependencies are not defined independently; they are fully implied by data-flow
dependencies.

### Execution implication

In Spider, a task is eligible to be scheduled for execution if and only if **one** of the following
conditions holds:

* The task is an input task, or
* All parent tasks of the task have completed successfully.

## Implementation requirements

:::{warning} 🚧 This section is still under construction. :::