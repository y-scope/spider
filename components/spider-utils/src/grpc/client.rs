//! A round-robin pool of gRPC service-client connections.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use tonic::transport::Channel;
use tonic::transport::Endpoint;

use super::Error;

/// A pool of independent gRPC connections to a single endpoint.
///
/// Each pooled client holds its own connection, so spreading requests across the pool avoids the
/// throughput bottleneck caused by [hyperium/h2#531](https://github.com/hyperium/h2/issues/531).
///
/// # Type Parameters
///
/// * `GrpcServiceClientType` - The gRPC service client type held by the pool.
#[derive(Clone, Debug)]
pub struct ConnectionPool<GrpcServiceClientType: Clone> {
    inner: Arc<ConnectionPoolInner<GrpcServiceClientType>>,
}

impl<GrpcServiceClientType: Clone> ConnectionPool<GrpcServiceClientType> {
    /// Builds a pool of `pool_size` independent connections to `endpoint`.
    ///
    /// Each connection is established eagerly, then handed to `client_factory` to build the service
    /// client that wraps it.
    ///
    /// # Type Parameters
    ///
    /// * `ClientFactory` - Builds a service client from a connected [`Channel`].
    ///
    /// # Returns
    ///
    /// A new [`ConnectionPool`] holding `pool_size` connected clients on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::TonicTransport`] if a connection to `endpoint` fails to establish.
    pub async fn connect<ClientFactory: Fn(Channel) -> GrpcServiceClientType>(
        endpoint: Endpoint,
        pool_size: NonZeroUsize,
        client_factory: ClientFactory,
    ) -> Result<Self, Error> {
        let mut connections = Vec::with_capacity(pool_size.get());
        for _ in 0..pool_size.get() {
            let channel = endpoint
                .clone()
                .connect()
                .await
                .map_err(Error::TonicTransport)?;
            connections.push(client_factory(channel));
        }

        Ok(Self {
            inner: Arc::new(ConnectionPoolInner {
                connections,
                next: AtomicUsize::new(0),
            }),
        })
    }

    /// Selects the next client from the pool in round-robin order.
    ///
    /// # Returns
    ///
    /// A clone of the next pooled client.
    #[must_use]
    pub fn get_client(&self) -> GrpcServiceClientType {
        let next = self
            .inner
            .next
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.connections[next % self.inner.connections.len()].clone()
    }
}

#[derive(Debug)]
struct ConnectionPoolInner<GrpcServiceClientType: Clone> {
    connections: Vec<GrpcServiceClientType>,
    next: AtomicUsize,
}
