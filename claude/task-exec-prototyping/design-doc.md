# TDL Package & Task Executor: Design Document

## Context

The spider system needs a way for users to write custom task functions in Rust, package them into
a shared library (cdylib), and have the task executor load and invoke these functions at runtime
via C-FFI. This design covers the two components we are prototyping:

1. **TDL Package** -- the shared library containing user-defined tasks, a `#[task]` proc-macro,
   a registration macro, and C-FFI entry points.
2. **Task Executor** -- driver code in the execution process that loads a TDL package via `dlopen`
   and calls its C-FFI APIs.

The executor receives a task name and serialized inputs (wire format), dispatches into the loaded
library, and receives serialized outputs or an error. Deserialization of outputs happens upstream
in the execution manager (out of scope for this prototype).

---

## 1. Crate Organization

Four new crates, added to the workspace:

| Crate | Path | Type | Purpose |
|---|---|---|---|
| `spider-tdl` | `components/spider-tdl` | `lib` | Shared types, traits, wire serde, registration macro |
| `spider-tdl-derive` | `components/spider-tdl-derive` | `proc-macro` | `#[task]` attribute macro |
| `spider-executor` | `components/spider-executor` | `lib` | Loads cdylib, calls C-FFI |
| `example-tdl-package` | `examples/example-tdl-package` | `cdylib` | Sample TDL package |

Dependency graph:

```
spider-tdl-derive  (proc-macro: syn, quote, proc-macro2)
       │
       ▼
  spider-tdl  (rmp-serde, serde, thiserror; re-exports spider-tdl-derive)
       │
       ├──► spider-executor  (libloading, thiserror)
       │
       └──► example-tdl-package  (cdylib; serde, rmp-serde)
```

`spider-tdl` is the single source of truth for all shared types (C-FFI structs, error type,
`Task` trait, `TaskHandler`, wire format). Both the TDL package and the executor depend on it.

Workspace root `Cargo.toml` change:

```toml
members = [
  "components/spider-core",
  "components/spider-derive",
  "components/spider-storage",
  "components/spider-tdl",
  "components/spider-tdl-derive",
  "components/spider-executor",
  "examples/example-tdl-package",
]
```

---

## 2. Module Structure

### 2.1 `spider-tdl`

```
components/spider-tdl/
  Cargo.toml
  src/
    lib.rs              # Re-exports all public API
    tdl_types.rs        # Type aliases (int8=i8, List<T>=Vec<T>, etc.)
    error.rs            # TdlError enum (shared across all components)
    task_context.rs     # TaskContext struct (runtime metadata, shared across all components)
    ffi.rs              # CArray, CCharArray, CByteArray, TaskExecutionResult (#[repr(C)])
    wire.rs             # Wire format serde (adapted from claude/struct-serde/example)
    task.rs             # Task trait, TaskHandler trait, TaskHandlerImpl<T>, ExecutionResult
    register.rs         # register_tasks! macro_rules
```

Dependencies:

```toml
[dependencies]
spider-core = { path = "../spider-core" }
spider-tdl-derive = { path = "../spider-tdl-derive" }
rmp-serde = "1.3.1"
serde = { version = "1.0.228", features = ["derive"] }
thiserror = "2.0.18"
```

### 2.2 `spider-tdl-derive`

```
components/spider-tdl-derive/
  Cargo.toml
  src/
    lib.rs              # #[proc_macro_attribute] pub fn task(...)
    task_macro.rs       # Core code generation logic
    validation.rs       # Type validation rules
```

Dependencies:

```toml
[lib]
proc-macro = true

[dependencies]
proc-macro2 = "1.0.106"
quote = "1.0.45"
syn = { version = "2.0.117", features = ["full"] }
```

### 2.3 `spider-executor`

```
components/spider-executor/
  Cargo.toml
  src/
    lib.rs
    loader.rs           # TdlPackageLoader: dlopen, symbol lookup, safe wrappers
    error.rs            # Executor-specific error type
```

Dependencies:

```toml
[dependencies]
spider-tdl = { path = "../spider-tdl" }
libloading = "0.8"
thiserror = "2.0.18"
```

### 2.4 `example-tdl-package`

```
examples/example-tdl-package/
  Cargo.toml
  src/
    lib.rs              # User structs, #[task] functions, register_tasks! invocation
```

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
spider-tdl = { path = "../../components/spider-tdl" }
rmp-serde = "1.3.1"
serde = { version = "1.0.228", features = ["derive"] }
```

---

## 3. Type Aliases (`tdl_types`)

```rust
// components/spider-tdl/src/tdl_types.rs

pub type int8 = i8;
pub type int16 = i16;
pub type int32 = i32;
pub type int64 = i64;
pub type float = f32;
pub type double = f64;
pub type boolean = bool;
pub type Bytes = Vec<u8>;
pub type List<T> = Vec<T>;
pub type Map<K, V> = std::collections::HashMap<K, V> where K: MapKey;

// --- Sealed marker trait restricting Map key types ---

mod private {
    pub trait Sealed {}
}

/// Marker trait for types allowed as `Map` keys.
/// Sealed — users cannot implement this for their own types.
pub trait MapKey: Eq + std::hash::Hash + private::Sealed {}

impl private::Sealed for i8 {}
impl private::Sealed for i16 {}
impl private::Sealed for i32 {}
impl private::Sealed for i64 {}
impl private::Sealed for Vec<u8> {}

impl MapKey for i8 {}
impl MapKey for i16 {}
impl MapKey for i32 {}
impl MapKey for i64 {}
impl MapKey for Vec<u8> {}
```

This provides two layers of key-type enforcement:
1. **Type-level** -- `Map<String, int32>` fails to compile (no `MapKey` impl for `String`).
2. **Proc-macro** -- catches it earlier with a clearer error message naming the offending parameter.

---

## 4. Error Type

```rust
// components/spider-tdl/src/error.rs

#[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
pub enum TdlError {
    #[error("task not found: {0}")]
    TaskNotFound(String),

    #[error("deserialization error: {0}")]
    DeserializationError(String),

    #[error("serialization error: {0}")]
    SerializationError(String),

    #[error("execution error: {0}")]
    ExecutionError(String),

    #[error("{0}")]
    Custom(String),
}
```

`TdlError` derives `Serialize + Deserialize` so it can be msgpack-encoded into
`ExecutionResult::Error(Vec<u8>)` and decoded on the executor side.

This is the error type that user task functions return: `fn my_task(...) -> Result<T, TdlError>`.

---

## 5. `TaskContext`

Every task function must accept `TaskContext` as its **first** parameter. It carries runtime
metadata about the current task execution, constructed by the execution manager.

```rust
// components/spider-tdl/src/task_context.rs

use spider_core::types::id::{JobId, TaskId, TaskInstanceId};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskContext {
    pub job_id: JobId,
    pub task_id: TaskId,
    pub task_instance_id: TaskInstanceId,
}
```

**Visibility:** Defined in `spider-tdl`, re-exported publicly. Visible to:
- **Execution manager** -- constructs and msgpack-serializes it.
- **Task executor** -- passes serialized bytes through to the cdylib (opaque).
- **TDL package** -- deserializes it from msgpack and passes to the user's task function.

**Serialization:** Plain msgpack (not wire format). A single `rmp_serde::to_vec(&ctx)` /
`rmp_serde::from_slice(&bytes)`. This is separate from the task inputs wire stream.

**Why separate from inputs:** `TaskContext` is runtime metadata owned by the execution manager,
not user-supplied data from the storage layer. It travels a different path -- the execution
manager constructs it and attaches it alongside the input wire bytes.

**Note on `TaskId`:** `TaskId` is already defined in `spider-core::types::id` (as
`Id<TaskIdMarker>`). No relocation from `spider-storage` is needed.

---

## 6. C-FFI Types (`ffi`)

All `#[repr(C)]` types shared between the TDL package and the executor.

```rust
// components/spider-tdl/src/ffi.rs

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CArray<'a, T> {
    pointer: *const T,
    length: usize,
    _lifetime: PhantomData<&'a [T]>,
}

impl<'a, T> CArray<'a, T> {
    pub fn from_slice(slice: &'a [T]) -> Self { ... }
    /// # Safety
    /// The pointer must be valid for `length` elements.
    pub unsafe fn as_slice(&self) -> &'a [T] { ... }
}

pub type CCharArray<'a> = CArray<'a, c_char>;
pub type CByteArray<'a> = CArray<'a, u8>;
```

For `CCharArray`, add a convenience:

```rust
impl<'a> CCharArray<'a> {
    pub fn from_str(s: &'a str) -> Self { ... }  // cast *const u8 to *const c_char
    /// # Safety
    /// The pointer must be valid UTF-8.
    pub unsafe fn as_str(&self) -> &'a str { ... }
}
```

### `TaskExecutionResult`

```rust
#[repr(C)]
pub struct TaskExecutionResult {
    is_error: bool,
    pointer: *mut u8,
    length: usize,
}
```

Construction (inside TDL package, leaks the Vec):

```rust
impl TaskExecutionResult {
    pub fn from_execution_result(result: ExecutionResult) -> Self {
        let (is_error, buffer) = match result {
            ExecutionResult::Outputs(bytes) => (false, bytes),
            ExecutionResult::Error(bytes) => (true, bytes),
        };
        let boxed: Box<[u8]> = buffer.into_boxed_slice();
        let len = boxed.len();
        let ptr = Box::into_raw(boxed) as *mut u8;
        Self { is_error, pointer: ptr, length: len }
    }
}
```

Consumption (inside executor, reclaims ownership):

```rust
impl TaskExecutionResult {
    /// Reclaim ownership of the byte buffer. Must only be called once.
    /// # Safety
    /// The pointer must have been produced by `from_execution_result`.
    pub unsafe fn into_result(self) -> Result<Vec<u8>, Vec<u8>> {
        let boxed = unsafe {
            Box::from_raw(std::slice::from_raw_parts_mut(self.pointer, self.length))
        };
        let vec = boxed.into_vec();
        if self.is_error { Err(vec) } else { Ok(vec) }
        // Note: must prevent Drop from double-freeing.
        // Use ManuallyDrop or mem::forget on self.
    }
}
```

Memory safety note: this works because both the cdylib and the executor are in the same process
and share the same Rust global allocator. The allocation is created on one side
(`Box::into_raw`) and freed on the other (`Box::from_raw`).

---

## 6. Serialization Formats

Adapted from `claude/struct-serde/example/src/lib.rs` into `components/spider-tdl/src/wire.rs`.

There are two serialization layers:

| Layer | Format | Purpose |
|-------|--------|---------|
| **Wire format** | Custom length-prefixed framing (u32 LE) | Frames a sequence of opaque payloads into a flat byte stream. Handles element boundaries. |
| **Payload format** | MessagePack (`rmp-serde`) | Encodes each individual value (input arg, output element, or error). Self-describing, compact binary. |

The wire format never interprets payload bytes -- it reads/writes them as opaque
`[len][data]` chunks. All type-aware serialization is done by msgpack at the payload layer.

### 6.1 Inputs

**Wire format:**

```
[count: u32 LE] [len₀: u32 LE][payload₀ …] [len₁: u32 LE][payload₁ …] …
```

- `count` -- number of task arguments (= number of `TaskInput` elements).
- Each `payload` is the raw `Vec<u8>` from `TaskInput::ValuePayload`, already msgpack-encoded
  by whichever component produced the `TaskInput`.
- Fixed-width u32 LE lengths. Faster to parse than varints; 4 bytes overhead per field is
  negligible.

**Where serialized:** Storage layer, via `serialize_task_inputs(&[TaskInput]) -> Vec<u8>`.

**Where deserialized:** TDL package (cdylib), via
`deserialize_task_inputs<'de, T: Deserialize<'de>>(data: &'de [u8]) -> Result<T, WireError>`.

**Avoiding memory copies on deserialization:**

The `StreamDeserializer` holds a borrowed `&'de [u8]` reference to the wire buffer. For each
field, it reads the u32 LE length, then yields a `&'de [u8]` slice pointing directly into the
original buffer (zero-copy). That slice is fed to `rmp_serde::Deserializer::from_read_ref()`,
which deserializes the field value from the borrowed slice. No intermediate `Vec<TaskInput>` is
constructed at the sink. Total copies per field: one (from msgpack into the target value).

```
wire buffer (borrowed &[u8]):
┌───────┬──────┬───────────┬──────┬───────────┬───┐
│ count │ len₀ │ payload₀  │ len₁ │ payload₁  │...│
└───────┴──────┴─────┬─────┴──────┴─────┬─────┴───┘
                     │                  │
              &'de [u8] slice    &'de [u8] slice    ← zero-copy borrows
                     │                  │
                     ▼                  ▼
              rmp_serde deser    rmp_serde deser     ← one deser per field
                     │                  │
                     ▼                  ▼
                field₀ value     field₁ value        ← target Params struct
```

### 6.2 Outputs

**Wire format:** Identical framing to inputs.

```
[count: u32 LE] [len₀: u32 LE][payload₀ …] [len₁: u32 LE][payload₁ …] …
```

- `count` -- number of tuple elements in the return type (known at compile time).
- Each `payload` is one tuple element, independently msgpack-encoded via `rmp_serde::to_vec()`.

**Where serialized:** TDL package (cdylib), in the proc-macro-generated `serialize_return()`.

**Where deserialized:** Storage layer, which reads the wire frame back into `Vec<Vec<u8>>`
(each element is one msgpack-encoded output value, kept as raw bytes for downstream use).

**Avoiding memory copies on serialization:**

The proc-macro generates per-task code that knows the exact tuple arity at compile time. Each
tuple element is serialized into a temporary `Vec<u8>` via `rmp_serde::to_vec()`, then the
framing is assembled. To minimize copies, the output buffer is pre-allocated to exact capacity:

```rust
// Generated by #[task] for return type (T0, T1):
fn serialize_return(result: &(T0, T1)) -> Result<Vec<u8>, TdlError> {
    let elem0 = rmp_serde::to_vec(&result.0)?;
    let elem1 = rmp_serde::to_vec(&result.1)?;

    // Pre-allocate exact size: header + (len + payload) per element
    let total = 4 + (4 + elem0.len()) + (4 + elem1.len());
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&2u32.to_le_bytes());       // count
    buf.extend_from_slice(&(elem0.len() as u32).to_le_bytes());
    buf.extend_from_slice(&elem0);                     // payload₀
    buf.extend_from_slice(&(elem1.len() as u32).to_le_bytes());
    buf.extend_from_slice(&elem1);                     // payload₁
    Ok(buf)
}
```

Each element requires exactly two copies: one from the value into the temporary msgpack
`Vec<u8>`, and one from that temporary into the output buffer. The output buffer never
reallocates (exact pre-allocation).

**Avoiding memory copies on deserialization (storage layer):**

The storage layer deserializes outputs into `Vec<Vec<u8>>` -- it only needs to parse the wire
framing, not the msgpack payloads. Each payload is copied once out of the wire buffer into its
own `Vec<u8>`. The msgpack bytes remain opaque at this layer.

### 6.3 Errors

**Format:** A single msgpack-encoded `TdlError` value (no wire framing).

```
[msgpack-encoded TdlError]
```

- No length-prefixed framing -- the entire byte buffer is one msgpack blob.
- `TdlError` derives `Serialize + Deserialize`, so `rmp_serde::to_vec(&err)` /
  `rmp_serde::from_slice(&bytes)` is all that is needed.

**Where serialized:** TDL package (cdylib), inside `TaskHandlerImpl::execute_raw()`, when the
task returns `Err(TdlError)` or deserialization/serialization fails.

**Where deserialized:** Task executor, after reclaiming the `TaskExecutionResult` buffer, to
determine the failure reason. The executor may also forward the raw error bytes to the execution
manager if needed.

### 6.4 Summary: what each component sees

```
                    TaskContext        Inputs              Outputs             Errors
                    ───────────       ──────              ───────             ──────
Storage             —                 Vec<TaskInput>      Vec<Vec<u8>>        —
                                      → serialize         ← deserialize
                                        (wire format)       (wire frame only)

Exec Manager        TaskContext        raw bytes           raw bytes           raw bytes
                    → serialize       (passthrough)       (passthrough)       (passthrough)
                      (msgpack)

Task Executor       raw bytes         raw bytes           raw bytes           TdlError
                    (passthrough)     (passthrough)       (passthrough)       ← deserialize
                                                                                (msgpack)

TDL Package         TaskContext        Params struct       Return tuple        TdlError
                    ← deserialize     ← deserialize       → serialize         → serialize
                      (msgpack)         (wire + msgpack)    (msgpack + wire)    (msgpack)
```

The wire format helper for output framing:

```rust
/// Serialize pre-encoded msgpack elements into wire format.
pub fn serialize_wire_frame(encoded_fields: &[&[u8]]) -> Vec<u8> {
    let total = 4 + encoded_fields.iter().map(|f| 4 + f.len()).sum::<usize>();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&(encoded_fields.len() as u32).to_le_bytes());
    for field in encoded_fields {
        buf.extend_from_slice(&(field.len() as u32).to_le_bytes());
        buf.extend_from_slice(field);
    }
    buf
}
```

The wire error type used in deserialization is specific to the wire module. It maps to
`TdlError` at the `TaskHandlerImpl` boundary.

---

## 7. `Task` Trait and `TaskHandler`

```rust
// components/spider-tdl/src/task.rs

pub enum ExecutionResult {
    Outputs(Vec<u8>),
    Error(Vec<u8>),
}

pub trait Task {
    const NAME: &'static str;
    type Params: for<'de> serde::Deserialize<'de>;
    type Return;

    fn execute(ctx: TaskContext, params: Self::Params) -> Result<Self::Return, TdlError>;

    /// Serialize the return tuple into wire format bytes.
    /// Generated by the #[task] proc-macro.
    fn serialize_return(result: &Self::Return) -> Result<Vec<u8>, TdlError>;
}

pub trait TaskHandler: Send + Sync {
    /// `raw_ctx` is msgpack-encoded `TaskContext`.
    /// `raw_args` is wire-format-encoded task inputs.
    fn execute_raw(&self, raw_ctx: &[u8], raw_args: &[u8]) -> ExecutionResult;
    fn name(&self) -> &'static str;
}

pub struct TaskHandlerImpl<T: Task> {
    _marker: PhantomData<T>,
}

impl<T: Task> TaskHandlerImpl<T> {
    pub fn new() -> Self {
        Self { _marker: PhantomData }
    }
}

impl<T: Task> TaskHandler for TaskHandlerImpl<T> {
    fn execute_raw(&self, raw_ctx: &[u8], raw_args: &[u8]) -> ExecutionResult {
        // 1. Deserialize TaskContext (msgpack)
        let ctx: TaskContext = match rmp_serde::from_slice(raw_ctx) {
            Ok(c) => c,
            Err(e) => {
                let err = TdlError::DeserializationError(
                    format!("failed to deserialize TaskContext: {e}")
                );
                return ExecutionResult::Error(rmp_serde::to_vec(&err).unwrap());
            }
        };

        // 2. Deserialize task inputs (wire format)
        let params: T::Params = match wire::deserialize_task_inputs(raw_args) {
            Ok(p) => p,
            Err(e) => {
                let err = TdlError::DeserializationError(e.to_string());
                return ExecutionResult::Error(rmp_serde::to_vec(&err).unwrap());
            }
        };

        // 3. Execute task with context
        match T::execute(ctx, params) {
            Ok(result) => {
                // 4. Serialize outputs
                match T::serialize_return(&result) {
                    Ok(bytes) => ExecutionResult::Outputs(bytes),
                    Err(e) => ExecutionResult::Error(rmp_serde::to_vec(&e).unwrap()),
                }
            }
            Err(e) => ExecutionResult::Error(rmp_serde::to_vec(&e).unwrap()),
        }
    }

    fn name(&self) -> &'static str {
        T::NAME
    }
}
```

---

## 8. `#[task]` Proc-Macro

### Input

The first parameter must always be `ctx: TaskContext`. Remaining parameters are the task's
user-supplied inputs, deserialized from the wire format.

```rust
#[task]
fn my_task(ctx: TaskContext, a: int32, b: MyStruct1) -> Result<(List<MyStruct2>, int64), TdlError> {
    // user body
}
```

Or with a custom name:

```rust
#[task(name = "my_namespace::my_task")]
fn my_task(ctx: TaskContext, a: int32, b: MyStruct1) -> Result<(List<MyStruct2>, int64), TdlError> { ... }
```

### Generated output

The proc-macro strips `ctx: TaskContext` from the params struct (it is not part of the wire
inputs) and threads it through to the user function separately.

```rust
/// Marker struct.
pub struct my_task;

impl my_task {
    /// The original user function, renamed.
    fn __my_task(ctx: TaskContext, a: int32, b: MyStruct1) -> Result<(List<MyStruct2>, int64), TdlError> {
        // original body
    }
}

/// Params struct for deserialization -- only the wire-format inputs, NOT TaskContext.
#[derive(serde::Deserialize)]
struct __my_task_params {
    a: int32,
    b: MyStruct1,
}

impl spider_tdl::Task for my_task {
    const NAME: &'static str = "my_task";  // or "my_namespace::my_task"
    type Params = __my_task_params;
    type Return = (List<MyStruct2>, int64);

    fn execute(ctx: spider_tdl::TaskContext, params: Self::Params) -> Result<Self::Return, spider_tdl::TdlError> {
        Self::__my_task(ctx, params.a, params.b)
    }

    fn serialize_return(result: &Self::Return) -> Result<Vec<u8>, spider_tdl::TdlError> {
        let map_err = |e: rmp_serde::encode::Error|
            spider_tdl::TdlError::SerializationError(e.to_string());
        let elem0 = rmp_serde::to_vec(&result.0).map_err(map_err)?;
        let elem1 = rmp_serde::to_vec(&result.1).map_err(map_err)?;
        Ok(spider_tdl::wire::serialize_wire_frame(&[&elem0, &elem1]))
    }
}
```

### No-input tasks (empty params)

When a task has no user-supplied inputs (only `TaskContext`):

```rust
#[task]
fn my_context_only_task(ctx: TaskContext) -> Result<(int32,), TdlError> {
    Ok((42,))
}
```

The generated params struct is empty:

```rust
#[derive(serde::Deserialize)]
struct __my_context_only_task_params {}

impl spider_tdl::Task for my_context_only_task {
    type Params = __my_context_only_task_params;
    // ...
    fn execute(ctx: spider_tdl::TaskContext, params: Self::Params) -> Result<Self::Return, spider_tdl::TdlError> {
        Self::__my_context_only_task(ctx)  // no params to unpack
    }
}
```

**Wire format for empty params:** The input wire bytes must still be a valid wire frame with
`count = 0`:

```
[0x00, 0x00, 0x00, 0x00]   ← count: u32 LE = 0, no field entries
```

This is 4 bytes total. The `StreamDeserializer` reads `count = 0`, validates
`0 == fields.len()` (the empty struct has 0 fields), and calls `visitor.visit_seq()` which
immediately returns `None` for `next_element_seed` -- producing the empty struct with no
deserialization work. The storage layer must produce this 4-byte wire frame even when
`Vec<TaskInput>` is empty.

### Validation rules (compile-time errors)

1. **First parameter must be `ctx: TaskContext`** -- the macro checks that the first argument's
   type is `TaskContext`. Compile error if missing or if `TaskContext` appears at any other
   position.
2. **Return type must be `Result<(...), TdlError>`** -- the Ok type must be a parenthesized
   tuple (even for single values: `Result<(int32,), TdlError>`).
3. **Argument types must be supported types** -- primitives, aliases (`int32`, `Bytes`, etc.),
   `Vec<T>`/`List<T>`, `HashMap<K,V>`/`Map<K,V>`, or user-defined structs (single-segment
   identifiers not in the primitive set, assumed valid). This applies to all parameters after
   `TaskContext`.
4. **Map key restriction** -- K must be one of `{i8, i16, i32, i64, int8, int16, int32, int64,
   Vec<u8>, Bytes}`.
5. **No `self` parameter** -- must be a free function.
6. **Tuple element types** follow the same validation as argument types.

Type alias resolution (e.g., `type MyInt = int32;`) is not possible at the syntactic level.
User-defined struct names pass validation and fail at serde time if incorrect.

---

## 9. `register_tasks!` Macro

A `macro_rules!` macro in `spider-tdl/src/register.rs`.

### Usage

```rust
spider_tdl::register_tasks! {
    package_name: "my_package",
    tasks: [my_task, another_task]
}
```

### Generated code

```rust
static __SPIDER_TDL_REGISTRY: std::sync::LazyLock<
    std::collections::HashMap<&'static str, Box<dyn spider_tdl::TaskHandler>>
> = std::sync::LazyLock::new(|| {
    let mut map = std::collections::HashMap::new();
    map.insert(
        <my_task as spider_tdl::Task>::NAME,
        Box::new(spider_tdl::TaskHandlerImpl::<my_task>::new()) as Box<dyn spider_tdl::TaskHandler>,
    );
    map.insert(
        <another_task as spider_tdl::Task>::NAME,
        Box::new(spider_tdl::TaskHandlerImpl::<another_task>::new()) as Box<dyn spider_tdl::TaskHandler>,
    );
    map
});

static __SPIDER_TDL_PACKAGE_NAME: &str = "my_package";

#[unsafe(no_mangle)]
pub extern "C" fn __spider_tdl_package_get_name<'a>() -> spider_tdl::ffi::CCharArray<'a> {
    spider_tdl::ffi::CCharArray::from_str(__SPIDER_TDL_PACKAGE_NAME)
}

#[unsafe(no_mangle)]
pub extern "C" fn __spider_tdl_package_execute(
    name: spider_tdl::ffi::CCharArray<'_>,
    ctx: spider_tdl::ffi::CByteArray<'_>,
    inputs: spider_tdl::ffi::CByteArray<'_>,
) -> spider_tdl::ffi::TaskExecutionResult {
    let task_name: &str = unsafe { name.as_str() };
    let raw_ctx: &[u8] = unsafe { ctx.as_slice() };
    let raw_inputs: &[u8] = unsafe { inputs.as_slice() };

    let result = match __SPIDER_TDL_REGISTRY.get(task_name) {
        Some(handler) => handler.execute_raw(raw_ctx, raw_inputs),
        None => {
            let err = spider_tdl::TdlError::TaskNotFound(task_name.to_string());
            spider_tdl::ExecutionResult::Error(rmp_serde::to_vec(&err).unwrap())
        }
    };

    spider_tdl::ffi::TaskExecutionResult::from_execution_result(result)
}
```

Uses `LazyLock` (stable since Rust 1.80) -- no extra dependency needed.
Uses `#[unsafe(no_mangle)]` per Rust 2024 edition.

---

## 10. Task Executor Driver

### `TdlPackageLoader`

```rust
// components/spider-executor/src/loader.rs

pub struct TdlPackageLoader {
    library: libloading::Library,
}

type GetNameFn = unsafe extern "C" fn() -> CCharArray<'static>;
type ExecuteFn = unsafe extern "C" fn(CCharArray<'_>, CByteArray<'_>, CByteArray<'_>) -> TaskExecutionResult;

impl TdlPackageLoader {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ExecutorError> {
        let library = unsafe { libloading::Library::new(path.as_ref()) }?;
        Ok(Self { library })
    }

    pub fn package_name(&self) -> Result<&str, ExecutorError> {
        unsafe {
            let func: libloading::Symbol<GetNameFn> =
                self.library.get(b"__spider_tdl_package_get_name")?;
            let name_arr = func();
            let bytes = name_arr.as_slice();
            std::str::from_utf8(bytes).map_err(|e| ExecutorError::InvalidUtf8(e))
        }
    }

    /// Execute a task by name.
    /// - `raw_ctx` is msgpack-encoded `TaskContext`, constructed by the execution manager.
    /// - `raw_inputs` is wire-format-encoded task inputs, produced by the storage layer.
    /// Both are passed through opaquely.
    pub fn execute_task(
        &self,
        task_name: &str,
        raw_ctx: &[u8],
        raw_inputs: &[u8],
    ) -> Result<Vec<u8>, TdlError> {
        unsafe {
            let func: libloading::Symbol<ExecuteFn> =
                self.library.get(b"__spider_tdl_package_execute")?;
            let name_arr = CCharArray::from_str(task_name);
            let ctx_arr = CByteArray::from_slice(raw_ctx);
            let input_arr = CByteArray::from_slice(raw_inputs);
            let result = func(name_arr, ctx_arr, input_arr);

            // Reclaim ownership and interpret
            match result.into_result() {
                Ok(output_bytes) => Ok(output_bytes),
                Err(error_bytes) => {
                    let tdl_error: TdlError = rmp_serde::from_slice(&error_bytes)?;
                    Err(tdl_error)
                }
            }
        }
    }
}
```

The executor does NOT deserialize the output bytes -- it passes them back to the execution
manager. It only deserializes error bytes to determine the failure reason.

---

## 11. End-to-End Data Flow

`Vec<TaskInput>` only exists in the storage layer. The storage layer serializes it into wire
bytes (`serialize_task_inputs`), and from that point on, only raw bytes flow through the system.

`TaskContext` is constructed and serialized by the execution manager. It travels separately
from the task inputs.

```
Storage              Execution Manager       Task Executor          TDL Package (cdylib)
   │                       │                       │                        │
   │ Vec<TaskInput>        │                       │                        │
   │ serialize_task_inputs │                       │                        │
   │ → input wire bytes    │                       │                        │
   │                       │                       │                        │
   │ task_name +           │                       │                        │
   │ input wire bytes      │                       │                        │
   ├──────────────────────►│                       │                        │
   │                       │                       │                        │
   │                       │ construct TaskContext  │                        │
   │                       │ rmp_serde::to_vec()   │                        │
   │                       │ → ctx bytes           │                        │
   │                       │                       │                        │
   │                       │ task_name +           │                        │
   │                       │ ctx bytes +           │                        │
   │                       │ input wire bytes      │                        │
   │                       ├──────────────────────►│                        │
   │                       │    (via OS pipe)      │                        │
   │                       │                       │                        │
   │                       │                       │ dlopen + symbol lookup │
   │                       │                       ├───────────────────────►│
   │                       │                       │ __spider_tdl_package_execute
   │                       │                       │ (CCharArray,           │
   │                       │                       │  CByteArray[ctx],      │
   │                       │                       │  CByteArray[inputs])   │
   │                       │                       │                        │
   │                       │                       │                        │ rmp_serde::from_slice()
   │                       │                       │                        │ ctx bytes → TaskContext
   │                       │                       │                        │
   │                       │                       │                        │ deserialize_task_inputs()
   │                       │                       │                        │ wire bytes → Params struct
   │                       │                       │                        │
   │                       │                       │                        │ Task::execute(ctx, params)
   │                       │                       │                        │ → Result<Return, TdlError>
   │                       │                       │                        │
   │                       │                       │                        │ serialize_return() or
   │                       │                       │                        │ serialize error
   │                       │                       │                        │ → ExecutionResult
   │                       │                       │                        │
   │                       │                       │ TaskExecutionResult    │
   │                       │                       │◄───────────────────────┤
   │                       │                       │ (repr(C), leaked Vec)  │
   │                       │                       │                        │
   │                       │                       │ into_result()          │
   │                       │                       │ → reclaim Vec<u8>      │
   │                       │                       │                        │
   │                       │ raw output/error bytes│                        │
   │                       │◄──────────────────────┤                        │
   │                       │    (via OS pipe)      │                        │
   │                       │                       │                        │
   │ raw output/error bytes│                       │                        │
   │◄──────────────────────┤                       │                        │
   │ (passthru from mgr)   │                       │                        │
   │                       │                       │                        │
   │ deserialize outputs   │                       │                        │
   │ wire bytes → Vec<Vec<u8>>                     │                        │
   │ (each element is one  │                       │                        │
   │  msgpack-encoded      │                       │                        │
   │  output value)        │                       │                        │
```

Key points:
- `serialize_task_inputs()` is called in the **storage layer**, not the executor.
- `TaskContext` is constructed and msgpack-serialized by the **execution manager**. It is
  passed as a separate byte stream alongside the input wire bytes.
- The task executor treats all three byte streams (ctx, inputs, outputs) as opaque passthrough.
- `deserialize_task_inputs()` is called in the **TDL package** (cdylib) only.
- Output bytes flow back opaquely to the **storage layer**, which deserializes the wire frame
  into `Vec<Vec<u8>>` (each element is one msgpack-encoded tuple output value).

---

## 12. Implementation Order

1. **`spider-tdl` foundation** -- `tdl_types.rs`, `error.rs`, `ffi.rs`
2. **Wire format** -- adapt `claude/struct-serde/example/src/lib.rs` into `wire.rs`; add
   `serialize_wire_frame` for output serialization
3. **Task traits** -- `task.rs` with `Task`, `TaskHandler`, `TaskHandlerImpl`, `ExecutionResult`
4. **Registration macro** -- `register.rs`
5. **Proc-macro** -- `spider-tdl-derive` with `task_macro.rs` and `validation.rs`
6. **Executor** -- `spider-executor` with `loader.rs`
7. **Example package** -- `examples/example-tdl-package`
8. **Integration test** -- build example cdylib, load it from executor, run a task end-to-end

---

## 13. Key Files to Reference

| File | Purpose |
|---|---|
| `claude/struct-serde/example/src/lib.rs` | Wire format serde to adapt into `wire.rs` |
| `components/spider-derive/src/lib.rs` | Proc-macro entry point pattern |
| `components/spider-derive/src/mysql.rs` | Proc-macro codegen pattern with syn/quote |
| `components/spider-core/src/types/io.rs` | `TaskInput`, `TaskOutput` definitions |
| `components/spider-core/src/task.rs` | Error enum pattern, module structure |

---

## 14. Verification Plan

1. **Unit tests in `spider-tdl`**: wire format round-trip, error serialization round-trip,
   `TaskExecutionResult` construction and reclamation.
2. **Proc-macro compile tests**: valid tasks compile; invalid return types, invalid map keys,
   self parameters produce compile errors.
3. **Integration test**: build `example-tdl-package` as cdylib, use `TdlPackageLoader` to load
   it, call `package_name()`, call `execute_task()` with serialized inputs, verify outputs
   deserialize correctly. Also test error paths (unknown task, deserialization failure).
