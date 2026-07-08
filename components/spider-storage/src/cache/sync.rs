use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;
use tokio::sync::RwLockWriteGuard;

/// Reader-writer lock for shared data in the cache.
pub type SharedRw<Type> = Arc<RwLock<Type>>;

/// A reader for shared data in the cache.
#[derive(Clone)]
pub struct Reader<Type: Send + Sync> {
    inner: Arc<RwLock<Type>>,
}

impl<Type: Send + Sync> Reader<Type> {
    /// Factory function.
    pub const fn new(inner: SharedRw<Type>) -> Self {
        Self { inner }
    }

    /// # Returns
    ///
    /// A guard that allows read access to the shared data. The guard will be released when it goes
    /// out of scope.
    pub async fn read(&self) -> RwLockReadGuard<'_, Type> {
        self.inner.read().await
    }
}

/// A writer for shared data in the cache.
#[derive(Clone)]
pub struct Writer<Type: Send + Sync> {
    inner: Arc<RwLock<Type>>,
}

impl<Type: Send + Sync> Writer<Type> {
    /// Factory function.
    pub const fn new(inner: SharedRw<Type>) -> Self {
        Self { inner }
    }

    /// # Returns
    ///
    /// A guard that allows write access to the shared data. The guard will be released when it goes
    /// out of scope.
    pub async fn write(&self) -> RwLockWriteGuard<'_, Type> {
        self.inner.write().await
    }
}
