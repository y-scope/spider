//! Rust gRPC protocol definitions generated from Spider protobuf files.

pub mod id;
pub mod job;

#[allow(clippy::all, clippy::nursery, clippy::pedantic)]
pub mod common {
    include!("generated/common.rs");
}

#[allow(clippy::all, clippy::nursery, clippy::pedantic)]
pub mod scheduler {
    include!("generated/scheduler.rs");
}

#[allow(clippy::all, clippy::nursery, clippy::pedantic)]
pub mod storage {
    include!("generated/storage.rs");
}
