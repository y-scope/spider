//! Rust gRPC protocol definitions generated from Spider protobuf files.

pub mod error;
pub mod id;
pub mod job;
pub mod payload;

#[allow(clippy::all, clippy::nursery, clippy::pedantic)]
pub mod storage {
    include!("generated/storage.rs");
}
