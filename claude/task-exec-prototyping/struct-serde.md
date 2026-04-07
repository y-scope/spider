# Positional Deserialization: `Vec<TaskInput>` Wire Format to Struct

## Problem

Given a `Vec<TaskInput>` (defined in `components/spider-core/src/types/io.rs`):

```rust
pub enum TaskInput {
    ValuePayload(Vec<u8>),  // each Vec<u8> is a msgpack-encoded field value
}
```

We want to:

1. **Serialize** the `Vec<TaskInput>` into a flat byte stream at the source.
2. **Deserialize** that byte stream directly into an arbitrary user-defined struct at the sink,
   where each struct field positionally consumes one `TaskInput`'s payload.

The sink struct only needs standard `#[derive(Deserialize)]` -- no custom derive macros.

## Two-Layer Design

There are two distinct serialization layers:

| Layer | Format | Purpose |
|-------|--------|---------|
| **Wire format** | Custom length-prefixed framing (u32 LE) | Encodes the `Vec<TaskInput>` sequence into a flat byte stream. Handles field boundaries. |
| **Payload format** | MessagePack (`rmp-serde`) | Encodes each individual field value inside its `ValuePayload`. Self-describing, compact binary. |

The wire format only frames the sequence of payloads. It never interprets the payload bytes --
it writes and reads them as opaque `[len][data]` chunks. All type-aware serialization is done
by msgpack at the payload layer.

## Wire Format

```text
[count: u32 LE] [len₀: u32 LE][data₀ …] [len₁: u32 LE][data₁ …] …
```

- `count` -- number of fields (= number of `TaskInput` elements).
- Each field is a `[len][data]` pair. `data` is the raw `Vec<u8>` from `ValuePayload`, written
  verbatim (it is already msgpack-encoded by whatever produced the `TaskInput`).
- Fixed-width u32 LE lengths. Faster to parse than varints; 4 bytes of overhead per field is
  negligible vs. actual payload.

## Data Flow

```text
Source                                       Sink
------                                       ----
Vec<TaskInput>                               &[u8] (the wire buffer)
     │                                            │
     ▼                                            ▼
serialize_task_inputs()               deserialize_task_inputs::<T>()
     │                                            │
     ▼                                            ▼
flat byte stream ───── network/disk ─────►  StreamDeserializer
                                                  │
                                            ┌─────┴──────────────┐
                                            │ For each field:    │
                                            │  read [len][data]  │
                                            │  data is &'de [u8] │  ← zero-copy slice
                                            │  into wire buffer  │
                                            │  rmp_serde::       │
                                            │   Deserializer     │
                                            │   ::from_read_ref()│  ← one deser per field
                                            └────────────────────┘
                                                  │
                                                  ▼
                                              T (the struct)
```

**Key property:** No intermediate `Vec<TaskInput>` is constructed at the sink. Each field's bytes
are a borrowed `&[u8]` slice into the original wire buffer, and rmp_serde deserializes from that
slice directly. One deserialization step per field, one memory copy per field.

## Implementation

### Serialization (source side)

Straightforward -- iterate and write length-prefixed chunks:

```rust
pub fn serialize_task_inputs(inputs: &[TaskInput]) -> Vec<u8> {
    let total: usize = 4 + inputs.iter().map(|i| {
        let TaskInput::ValuePayload(b) = i;
        4 + b.len()
    }).sum::<usize>();

    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&(inputs.len() as u32).to_le_bytes());
    for input in inputs {
        let TaskInput::ValuePayload(bytes) = input;
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(bytes);
    }
    buf
}
```

### Deserialization (sink side) -- custom serde Deserializer

The entry point:

```rust
pub fn deserialize_task_inputs<'de, T: Deserialize<'de>>(data: &'de [u8]) -> Result<T, Error> {
    let mut de = StreamDeserializer::new(data)?;
    T::deserialize(&mut de)
}
```

#### `StreamDeserializer`

Holds the wire buffer and a cursor. Only implements `deserialize_struct`; all other
`deserialize_*` methods forward to an error via `forward_to_deserialize_any!`.

```rust
struct StreamDeserializer<'de> {
    data: &'de [u8],
    pos: usize,
    count: usize,          // from the wire header
    current_field: usize,
    type_name: &'static str,
    field_names: &'static [&'static str],
}
```

`deserialize_struct` validates `count == fields.len()` (producing a `LengthMismatch` error on
mismatch), then calls `visitor.visit_seq(FieldSeqAccess { ... })`.

#### `FieldSeqAccess` (the core)

Implements `serde::de::SeqAccess`. Each call to `next_element_seed`:

1. Reads the next `[len: u32 LE][data: &'de [u8]]` from the buffer.
2. Creates a `rmp_serde::Deserializer::from_read_ref(data)`.
3. Calls `seed.deserialize(&mut rmp_de)` -- serde routes this to rmp_serde, which
   deserializes the field value from the borrowed slice.
4. Maps `rmp_serde::decode::Error` to our `Error::FieldDeserialization { type_name, field, position, .. }`.

```rust
impl<'a, 'de> SeqAccess<'de> for FieldSeqAccess<'a, 'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self, seed: T,
    ) -> Result<Option<T::Value>, Error> {
        if self.de.current_field >= self.de.count {
            return Ok(None);
        }
        let idx = self.de.current_field;
        let field_name = self.de.field_names.get(idx).copied().unwrap_or("<unknown>");
        let type_name = self.de.type_name;

        let bytes = self.de.next_field_bytes()?;  // &'de [u8] into wire buffer
        self.de.current_field += 1;

        let mut rmp_de = rmp_serde::Deserializer::from_read_ref(bytes);
        seed.deserialize(&mut rmp_de)
            .map(Some)
            .map_err(|e| Error::FieldDeserialization {
                type_name, field: field_name, position: idx, source: e,
            })
    }
}
```

**Why `seed.deserialize(&mut rmp_de)` works across error types:**
`DeserializeSeed::deserialize` is generic over `D: Deserializer<'de>` and returns
`Result<Value, D::Error>`. Here `D` is `&mut rmp_serde::Deserializer<...>`, so it returns
`Result<Value, rmp_serde::decode::Error>`. We `.map_err()` that into our `Error` at the
`SeqAccess` boundary. The error types don't need to match inside `seed.deserialize` -- they
only need to match the `SeqAccess::Error` associated type on the way out.

### Error Type

```rust
pub enum Error {
    LengthMismatch { type_name, expected, actual },
    FieldDeserialization { type_name, field, position, source: rmp_serde::decode::Error },
    InvalidFormat(&'static str),    // wire buffer corruption
    Custom(String),                 // required by serde::de::Error
}
```

Must implement `serde::de::Error` (for the `custom()` constructor) and `std::fmt::Display`.

## Usage

```rust
use serde::Deserialize;

// Sink defines its own struct -- only needs standard serde.
#[derive(Deserialize)]
struct Job {
    name: String,       // consumes inputs[0]
    priority: u32,      // consumes inputs[1]
    payload: Vec<u8>,   // consumes inputs[2]
}

// Source side: each TaskInput::ValuePayload contains rmp_serde::to_vec()-encoded bytes.
let wire: Vec<u8> = serialize_task_inputs(&task_inputs);

// Sink side (e.g. after receiving `wire` over the network):
let job: Job = deserialize_task_inputs(&wire)?;
```

Fields with complex types (nested structs, enums, `Option<T>`, etc.) work automatically -- each
field's `ValuePayload` is a self-contained msgpack blob, and rmp_serde handles the inner
structure. Because msgpack is self-describing, the payload layer is more resilient to type
mismatches than non-self-describing formats.

## Working Example

A compilable and tested example crate lives at `claude/struct-serde/example/`.
Dependencies: `rmp-serde = "1"`, `serde = "1"` (with `derive` feature).
