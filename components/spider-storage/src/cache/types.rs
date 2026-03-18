use std::sync::Arc;

use serde::Serialize;
use spider_core::{
    task::TaskIndex,
    types::{id::TaskInstanceId, io::TaskInput},
};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub type Shared<Type> = Arc<RwLock<Type>>;

#[derive(Clone)]
pub struct Reader<Type> {
    inner: Shared<Type>,
}

impl<Type> Reader<Type> {
    pub const fn new(inner: Shared<Type>) -> Self {
        Self { inner }
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, Type> {
        self.inner.read().await
    }
}

#[derive(Clone)]
pub struct Writer<Type> {
    inner: Shared<Type>,
}

impl<Type> Writer<Type> {
    pub const fn new(inner: Shared<Type>) -> Self {
        Self { inner }
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, Type> {
        self.inner.write().await
    }
}

#[derive(Serialize, Clone)]
pub struct TdlContext {
    pub(super) package: String,
    pub(super) func: String,
}

#[derive(Serialize)]
pub struct ExecutionContext {
    pub task_instance_id: TaskInstanceId,
    pub tdl_context: TdlContext,
    pub inputs: Option<Vec<TaskInput>>,
}

#[derive(Serialize, Clone)]
pub enum TaskId {
    TaskIndex(TaskIndex),
    Commit,
    Cleanup,
}
