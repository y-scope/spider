use std::marker::PhantomData;

use uuid::Uuid;

/// A generic identifier type that wraps a UUID and a type marker.
///
/// # Type Parameters:
///
/// * [`TypeMarker`]: A marker type used to differentiate between different types of IDs.
///
/// # Examples
///
/// ```rust
/// enum SomeTypeIdMarker {}
/// type SomeTypeId = Id<SomeTypeIdMarker>;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Id<TypeMarker>(Uuid, PhantomData<TypeMarker>);

impl<TypeMarker> Default for Id<TypeMarker> {
    fn default() -> Self {
        Self::new()
    }
}

impl<TypeMarker> Id<TypeMarker> {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4(), PhantomData)
    }

    #[must_use]
    pub const fn from(uid: Uuid) -> Self {
        Self(uid, PhantomData)
    }

    #[must_use]
    pub const fn as_uuid_ref(&self) -> &Uuid {
        &self.0
    }
}

pub enum ResourceGroupIdMarker {}
pub type ResourceGroupId = Id<ResourceGroupIdMarker>;

pub enum TaskIdMarker {}
pub type TaskId = Id<TaskIdMarker>;

pub enum JobIdMarker {}
pub type JobId = Id<JobIdMarker>;

pub enum DataIdMarker {}
pub type DataId = Id<DataIdMarker>;

pub enum WorkerIdMarker {}
pub type WorkerId = Id<WorkerIdMarker>;

pub enum TaskInstanceIdMarker {}
pub type TaskInstanceId = Id<TaskInstanceIdMarker>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_basic() {
        type TestId = Id<()>;
        let id = TestId::new();
        let underlying_uuid = id.as_uuid_ref().to_owned();
        assert_eq!(id, TestId::from(underlying_uuid));
    }
}
