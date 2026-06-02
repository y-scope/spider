//! gRPC protocol definitions for Spider storage services.

#[allow(clippy::all, clippy::nursery, clippy::pedantic)]
pub mod generated {
    include!("generated/spider.storage.rs");
}

pub mod storage;
