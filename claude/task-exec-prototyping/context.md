I'm designing a task execution package that allows users to write their own task functions with
custom function signatures using specific types. The high-level goal is to organize these
user-defined functions into a single package, exposing C-ffi APIs to call these methods with runtime
inputs and outputs. I will work through the design in detail. Your job is to help me implement such
a prototype. This package will be implemented in Rust.

1. Type system

The package would only support the following types, with the base types aliased to Rust's built-in
types:

- Primitive types:
  - `int8`: `i8`
  - `int16`: `i16`
  - `int32`: `i32`
  - `int64`: `i64`
  - `double`: `f64`
  - `float`: `f32`
  - `Bytes`: `Vec<u8>`
  - `boolean`: `bool`
- `List<T>`: `Vec<T>`, where `T` is any supported type.
- `Map<K, V>`: `HashMap<K, V>`, where `K` must be one of {`int8`, `int16`, `int32`, `int64`,
  `Bytes`} and `V` is any supported type.
- User-defined structs, where the struct contains only supported types, with each field uniquely
  identified by a name.

You might want to create these type aliases in the `tdl_types` module.

User defined task function is expected to have the following signature:

```rust
fn my_task(args...) -> Result<T, Error>;
```

Where:

- `args...` are positional arguements. Each must be one of the supported types.
- `T` is the return type of the function. It can be any supported type, or a tuple of supported
  types.
- `Error` is the error type of the function, defined in the package. It contains a custom variant
  that allows users to return a custom error message as a `String`.

All types, including the result type and the arguments, must support serialization and
deserialization using `serde`. This is because the task function will receive inputs as serialized
msgpack bytes from the C-ffi layer, and return outputs (the result) as serialized msgpack bytes.
We will go through this in detail in a later section.

2. `task` proc-macro

First, we will define a type trait for user-defined types.

```rust
trait Task {
    /// The name of the task.
    const NAME: &'static str;
    
    /// The parameters of the task.
    type Params: for<'de> Deserialize<'de>;
    
    /// The return type of the task.
    type Return: Serialize + for<'de> Deserialize<'de>;
    
    fn execute(args: Self::Params) -> Result<Self::Return, Error>;
}
```

We need a `task` proc-macro that automatically generates the task execution driver code on top of
the user-defined task function. With the following task:

```rust
fn my_task(a: int32, b: MyStruct1, ...) -> Result<(List<MyStruct2>), Error> {
    // User implementation.
}
```

The proc-macro will geneerate the following code:

```rust
/// An empty struct as a type marker, with the same name as the task.
pub(crate) struct my_task {}

impl my_task {
    /// User implementation
    fn __my_task(a: int32, b: MyStruct1, ...) -> Result<(List<MyStruct2>, int64), Error> {
        // User implementation.
    }
}

/// The parameters of the task, mirrored in the function signature.
#[derive(Deserialize)]
struct __my_task_params {
    a: int32,
    b: MyStruct1,
    ...
}

impl Task for my_task {
    // The exact name of the task defined by the user, if not specified.
    const NAME: &'static str = "my_task";
    
    // The parameters of the task
    type Params = __my_task_params;
    
    // The return type of the task
    type Return = (List<MyStruct2>, int64);
    
    fn execute(args: Self::Params) -> Result<Self::Return, Error> {
        // Call the user implementation with the deserialized parameters.
        Self::__my_task(args.a, args.b, ...)
    }
}
```

Requirements:

- Invalid types should be rejected, including both the args and the return type.
- The proc-macro should accept a `name` argument, allowing users to specify a custom name for the
  task that may contain namespace information.
- The return type should always be a tuple, even it only returns a single value. This is to simplify
  the result deserialization logic.

3. Task execution handler

On top of `Task`, we need a `TaskHandler` trait that wraps the input/output serialization and
deserialization.

```rust
enum ExecutionResult {
    Outputs(Vec<u8>),
    Error(Vec<u8>),
}

trait TaskHandler: {
    fn execute(&self, serialized_inputs: &[u8]) -> Vec<u8>;

    fn name(&self) -> &'static str;
}

struct TaskHandlerImpl<T: Task> {
    _marker: std::marker::PhantomData<T>,
}

impl<T: Task> TaskHandlerImpl<T> {
    fn new() -> Self {
        Self { _marker: std::marker::PhantomData }
    }
}

impl<T: Task> TaskHandler for TaskHandlerImpl<T>
{
    fn execute_raw(&self, raw_args: &[u8]) -> ExecutionResult {
        // 1. Deserialize the input bytes into the parameters.
        // The input bytes are serialized `TaskInput`. For this deserialization, check the doc under
        // `claude/task-exec-prototyping/struct-serde.md`.
        let params: T::Params = ...;

        // 2. Execute the task
        let result = T::default().execute(params)?;

        // 3. Serialize the result into `ExecutionResult`.
        // If the result is Ok, serialize the output value into bytes and return
        // `ExecutionResult::Outputs`. Since the return type is always a tuple, we want to serialize
        // each element inside the tuple into msgpack bytes independently, and then serialize them
        // using wire format. Please come up with a design that can avoid double memory copying: the
        // serialization should be streamingly appended into the output buffer, similar to the input
        // deserialization as mentioned in previous.
        // On error, serialize the error message into bytes using msgpack and return
        // `ExecutionResult::Error`.
    }

    fn name(&self) -> &'static str {
        T::NAME
    }
}
```

Read `exection_raw` carefully to generate a correct implementation.

4. Task registration

We need a macro to register the task functions into a package. The task package should accept a name
and the task objects (converted by the proc macro in step 2, which has the same name as the
user-defined task function).

Under the hood, the macro should generate a global hashmap that maps the task name (`Task::NAME`) to
a `dyn TaskHandler` for later access. The registration should also generate C-ffi APIs to access
task functions by given task name and inputs.

In a library, there can be only one task package.

The C-ffi APIs required:

(1). `__spider_tdl_package_get_name` which returns a byte view of the package name.
(2). `__spider_tdl_package_execute` which takes in a task name and serialized input bytes, and
returns serialized output bytes.

Specification for `__spider_tdl_package_execute`:

(1) Inputs:

Inputs are given with the following C-ffi type:

```rust
/// Represents a C `T const*` pointer + `size_t` length as a single ABI-stable value.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CArray<'lifetime, T> {
	pointer: *const T,
	length: usize,
	_lifetime: PhantomData<&'lifetime [T]>,
}

pub type CCharArray<'lifetime> = CArray<'lifetime, c_char>;

pub type CByteArray<'lifetime> = CArray<'lifetime, u8>;
```

The name is given as `CCharArray`, and the input bytes are given as `CByteArray`.

The API may convert the input bytes into the corresponding Rust reference, as `&str` and `&[u8]`.

(2) Outputs:

Outputs are given with the following C-ffi type:

```rust
#[repr(C)]
struct TaskExecutionResult {
    is_error: bool,
    pointer: *mut u8,
    length: usize,
}

impl TaskExecutionResult {
    pub fn new(result: ExecutionResult) {
        /*
        Something like:
        let is_error = match result {
            ExecutionResult::Outputs(_) => false,
            ExecutionResult::Error(_) => true,
        };
        let buffer = match result {
            ExecutionResult::Outputs(bytes) => bytes,
            ExecutionResult::Error(bytes) => bytes,
        };
        You may need to implement them properly
        */
        let buffer: *mut [u8] = Box::into_raw(buffer);
        Self {
            is_error,
            pointer: buffer as *mut u8, // use https://doc.rust-lang.org/nightly/std/primitive.pointer.html#method.as_mut_ptr-1 when stabilized -__-
            length: buffer.len(),
        }
    }

    // by not using `self`, forces callers to do `OwnedSlice::into_raw(buffer)` instead of `buffer.into_raw()`;
    // up to preference, this below follows rust std convention
    pub fn into_raw(this: Self) -> Box<[u8]> {
        unsafe {
            Box::from_raw(std::slice::from_raw_parts_mut(this.pointer, this.length))
        }
    }
}

impl Drop for TaskExecutionResult {
    fn drop(&mut self) {
        unsafe {
            Box::from_raw(std::slice::from_raw_parts_mut(this.pointer, this.length))
        }
    }
}
```

The API should convert the return properly.

The C-ffi API may call the task table's method directly, which abstracts the situation where the
task is not found (and thus returned as an error of `ExecutionResult::Error`).

5. End-to-end flow

Let's now review the end-to-end flow. In this session, you only need to focus on a small set of this
flow, but I'd like to list it here for you to have a better understanding.

The overall system has three components: the storage, the execution manager, and the actual
execution process. Each is an individual process.

Step 1: The execution manager requests a task from the storage, with the task name and its inputs.
Step 2: The execution manager passes the task into the execution process (through OS PIPE).
Step 3: The execution process loads the shared library that contains the task function (identified
by the task package name), and calls the task function with the inputs within the same process
through C-ffi APIs.
Step 4: The execution process retrieves the outputs from the task function, and returns them to the
execution manager (through OS PIPE).
Step 5: Depending on the error or success, the execution manager updates the task status in the
storage.

Note:
1. The execution results are generated in the task package lib. The ownership transfers from the lib
to the execution process across C-ffi. The execution process dispatches the results to see if it's
an error or not, and only notify the execution manager with the payload. The lifetime of the result
ends inside the execution process. This means the C-ffi `TaskExecutionResult` definition must be
visible in both the execution process and the separately compiled task package lib.
2. The error type, `Error`, will be deserialized inside the execution process. This means the error
must be defined in the task package lib, the execution process, and the execution manager.
3. The serialized output bytes are not deserialized until it reaches the execution manager.
4. The execution manager may need to maintain multiple TDL packages (loaded at runtime), but it
should only be asked to execute one task at a time.

In this session, you only need to implement the task package lib and the driver code in the
execution process that interacts with the lib. You don't need to worry about the rest, but you
should generate the overall flow diagram and make sure the design doesn't violate with the large
picture.

---

Your first task will be to come up with a formal design doc with the plan for how to implement the
package lib and the driver code inside the execution process. Put the design doc in the
`claude/task-exec-prototyping/design-doc.md` file.

We should refer the package lib as "TDL package", and the execution process as "task executor".

We will ask you to implement a prototype later.