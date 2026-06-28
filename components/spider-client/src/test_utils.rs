//! Shared helpers for the in-process mock gRPC unit tests in [`crate::job`] and
//! [`crate::resource_group`].

use std::net::SocketAddr;

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
/// Returns an error if the socket address cannot be parsed or [`TcpIncoming::bind`] fails to open
/// the listener.
pub fn bind_ephemeral() -> anyhow::Result<(SocketAddr, TcpIncoming)> {
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse()?)?.with_nodelay(Some(true));
    let addr = incoming.local_addr()?;
    Ok((addr, incoming))
}
