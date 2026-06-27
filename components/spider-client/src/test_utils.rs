//! Shared helpers for the in-process mock gRPC unit tests in [`crate::job`] and
//! [`crate::resource_group`].

use std::net::SocketAddr;

use tokio::net::TcpListener;
use tonic::{Status, transport::server::TcpIncoming};

/// The canned response a mock RPC returns: either a success value or a gRPC [`Status`] error.
pub enum MockResponse<T> {
    Success(T),
    Error(Status),
}

/// Binds an ephemeral loopback port and returns the address plus the bound incoming stream.
///
/// The caller wires the [`TcpIncoming`] into a `tonic` server task (see the `serve` helpers in
/// [`crate::job::tests`] and [`crate::resource_group::tests`]).
///
/// # Returns
///
/// The bound socket address and the [`TcpIncoming`] to feed a `tonic` server on success.
///
/// # Errors
///
/// Returns an error if the listener cannot be bound or converted to a tonic incoming stream.
/// `TcpIncoming::from_listener` returns `tonic::Error` (a `Box<dyn Error + Send + Sync>`), which is
/// not a `std::error::Error` and so cannot use `?` directly — it is folded into `anyhow::Error`
/// via [`anyhow::Error::msg`].
pub async fn bind_ephemeral() -> anyhow::Result<(SocketAddr, TcpIncoming)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = TcpIncoming::from_listener(listener, true, None).map_err(anyhow::Error::msg)?;
    Ok((addr, incoming))
}
