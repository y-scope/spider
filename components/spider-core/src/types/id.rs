use std::{fmt::Debug, marker::PhantomData};

use serde::{Deserialize, Serialize};
use sqlx::{Database, encode::IsNull};
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
/// #[derive(Debug, PartialEq, Eq)]
/// enum SomeTypeIdMarker {}
/// type SomeTypeId = Id<SomeTypeIdMarker>;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Id<TypeMarker: Debug + PartialEq + Eq>(Uuid, PhantomData<TypeMarker>);

impl<TypeMarker: Debug + PartialEq + Eq> Default for Id<TypeMarker> {
    fn default() -> Self {
        Self::new()
    }
}

impl<TypeMarker: Debug + PartialEq + Eq> Id<TypeMarker> {
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

    #[must_use]
    pub const fn as_bytes(&self) -> &UuidBytes {
        self.0.as_bytes()
    }
}

impl<TypeMarker, Db> sqlx::Type<Db> for Id<TypeMarker>
where
    TypeMarker: Debug + PartialEq + Eq,
    Db: Database,
    Uuid: sqlx::Type<Db>,
{
    fn type_info() -> <Db as Database>::TypeInfo {
        <Uuid as sqlx::Type<Db>>::type_info()
    }

    fn compatible(ty: &<Db as Database>::TypeInfo) -> bool {
        <Uuid as sqlx::Type<Db>>::compatible(ty)
    }
}

impl<'encode, TypeMarker, Db> sqlx::Encode<'encode, Db> for Id<TypeMarker>
where
    TypeMarker: Debug + PartialEq + Eq,
    Db: Database,
    Uuid: sqlx::Encode<'encode, Db>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <Db as Database>::ArgumentBuffer<'encode>,
    ) -> Result<IsNull, sqlx::error::BoxDynError> {
        self.0.encode_by_ref(buf)
    }
}

impl<'decode, TypeMarker, Db> sqlx::Decode<'decode, Db> for Id<TypeMarker>
where
    TypeMarker: Debug + PartialEq + Eq,
    Db: Database,
    Uuid: sqlx::Decode<'decode, Db>,
{
    fn decode(
        value: <Db as Database>::ValueRef<'decode>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        Uuid::decode(value).map(|uuid| Self(uuid, PhantomData))
    }
}

pub type UuidBytes = uuid::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceGroupIdMarker {}
pub type ResourceGroupId = Id<ResourceGroupIdMarker>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskIdMarker {}
pub type TaskId = Id<TaskIdMarker>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobIdMarker {}
pub type JobId = Id<JobIdMarker>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataIdMarker {}
pub type DataId = Id<DataIdMarker>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerIdMarker {}
pub type WorkerId = Id<WorkerIdMarker>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerIdMarker {}
pub type SchedulerId = Id<SchedulerIdMarker>;

pub type TaskInstanceId = u64;

/// Represents a signed ID.
///
/// In the Spider scheduling framework, resources are owned by resource groups. Many operations
/// require both the resource group ID and the resource's own ID to enforce proper access control.
/// This struct encapsulates both identifiers for such operations by treating the resource group ID
/// as the signature.
///
/// # Type Parameters
///
/// * [`TypeMarker`] - A marker type used to differentiate between different resource types.
pub struct SignedId<TypeMarker>
where
    TypeMarker: Debug + PartialEq + Eq, {
    signature: ResourceGroupId,
    id: Id<TypeMarker>,
}

impl<TypeMarker> SignedId<TypeMarker>
where
    TypeMarker: Debug + PartialEq + Eq,
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created instance of [`SignedId`].
    #[must_use]
    pub const fn new(signature: ResourceGroupId, id: Id<TypeMarker>) -> Self {
        Self { signature, id }
    }

    /// # Returns
    ///
    /// A reference to the underlying signature.
    #[must_use]
    pub const fn get_signature(&self) -> &ResourceGroupId {
        &self.signature
    }

    /// # Returns
    ///
    /// A reference to the underlying raw ID.
    #[must_use]
    pub const fn get(&self) -> &Id<TypeMarker> {
        &self.id
    }
}

pub type SignedJobId = SignedId<JobIdMarker>;

pub type SignedTaskId = SignedId<TaskIdMarker>;

#[cfg(test)]
mod tests {
    use std::any::TypeId;

    use super::*;

    #[test]
    fn test_id_basic() {
        let id = TaskId::new();
        let underlying_uuid = id.as_uuid_ref().to_owned();
        assert_eq!(id, TaskId::from(underlying_uuid));

        assert_ne!(TypeId::of::<TaskId>(), TypeId::of::<JobId>());
    }

    #[test]
    fn task_id_json_roundtrip() {
        let id = TaskId::new();
        let deserialized_id: TaskId = serde_json::from_str(
            serde_json::to_string(&id)
                .expect("JSON serialization failure")
                .as_str(),
        )
        .expect("JSON deserialization failure");
        assert_eq!(id, deserialized_id);
    }
}
