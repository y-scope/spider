use std::fmt::Debug;
use std::fmt::Display;
use std::marker::PhantomData;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use sqlx::Database;
use sqlx::encode::IsNull;

use crate::task::TaskIndex;

/// A generic identifier type that wraps a numeric ID and a type marker.
///
/// # Type Parameters:
///
/// * [`TypeMarker`]: A marker type used to differentiate between different types of IDs.
///
/// # Examples
///
/// ```rust
/// use spider_core::types::id::Id;
///
/// #[derive(Debug, PartialEq, Eq)]
/// enum SomeTypeIdMarker {}
/// type SomeTypeId = Id<SomeTypeIdMarker>;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Id<TypeMarker: Debug + PartialEq + Eq> {
    raw: u64,
    _marker: PhantomData<TypeMarker>,
}

impl<TypeMarker: Debug + PartialEq + Eq> Default for Id<TypeMarker> {
    fn default() -> Self {
        Self::from(0)
    }
}

impl<TypeMarker: Debug + PartialEq + Eq> Id<TypeMarker> {
    /// Creates a random ID for tests.
    ///
    /// Production IDs should be assigned by persistent storage instead.
    #[must_use]
    pub fn random() -> Self {
        Self::from(rand::random())
    }

    #[must_use]
    pub const fn from(id: u64) -> Self {
        Self {
            raw: id,
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub const fn get(&self) -> u64 {
        self.raw
    }
}

impl<TypeMarker: Debug + PartialEq + Eq, Db: Database> sqlx::Type<Db> for Id<TypeMarker>
where
    u64: sqlx::Type<Db>,
{
    fn type_info() -> <Db as Database>::TypeInfo {
        <u64 as sqlx::Type<Db>>::type_info()
    }

    fn compatible(ty: &<Db as Database>::TypeInfo) -> bool {
        <u64 as sqlx::Type<Db>>::compatible(ty)
    }
}

impl<'encode, TypeMarker: Debug + PartialEq + Eq, Db: Database> sqlx::Encode<'encode, Db>
    for Id<TypeMarker>
where
    u64: sqlx::Encode<'encode, Db>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <Db as Database>::ArgumentBuffer<'encode>,
    ) -> Result<IsNull, sqlx::error::BoxDynError> {
        self.get().encode_by_ref(buf)
    }
}

impl<'decode, TypeMarker: Debug + PartialEq + Eq, Db: Database> sqlx::Decode<'decode, Db>
    for Id<TypeMarker>
where
    u64: sqlx::Decode<'decode, Db>,
{
    fn decode(
        value: <Db as Database>::ValueRef<'decode>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        u64::decode(value).map(|id| Self::from(id))
    }
}

impl<TypeMarker: Debug + PartialEq + Eq> Display for Id<TypeMarker> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.get(), formatter)
    }
}

impl<TypeMarker: Debug + PartialEq + Eq> Serialize for Id<TypeMarker> {
    fn serialize<SerializerImpl: Serializer>(
        &self,
        serializer: SerializerImpl,
    ) -> Result<SerializerImpl::Ok, SerializerImpl::Error> {
        self.get().serialize(serializer)
    }
}

impl<'deserializer_lifetime, TypeMarker: Debug + PartialEq + Eq> Deserialize<'deserializer_lifetime>
    for Id<TypeMarker>
{
    fn deserialize<DeserializerImpl: Deserializer<'deserializer_lifetime>>(
        deserializer: DeserializerImpl,
    ) -> Result<Self, DeserializerImpl::Error> {
        u64::deserialize(deserializer).map(Self::from)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceGroupIdMarker {}
pub type ResourceGroupId = Id<ResourceGroupIdMarker>;

/// Identifier of a task inside a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskId {
    /// The index of the task in the job's task graph.
    Index(TaskIndex),

    /// The commit task.
    Commit,

    /// The cleanup task.
    Cleanup,
}

impl Display for TaskId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Index(index) => write!(formatter, "{index}"),
            Self::Commit => write!(formatter, "commit"),
            Self::Cleanup => write!(formatter, "cleanup"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobIdMarker {}
pub type JobId = Id<JobIdMarker>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataIdMarker {}
pub type DataId = Id<DataIdMarker>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionManagerIdMarker {}
pub type ExecutionManagerId = Id<ExecutionManagerIdMarker>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchedulerIdMarker {}
pub type SchedulerId = Id<SchedulerIdMarker>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskAssignmentIdMarker {}
pub type TaskAssignmentId = Id<TaskAssignmentIdMarker>;

pub type SessionId = u64;

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

#[cfg(test)]
mod tests {
    use super::JobId;
    use super::ResourceGroupId;

    #[test]
    fn id_serializes_as_u64() {
        let job_id = JobId::from(42);
        let serialized =
            serde_json::to_string(&job_id).expect("job id serialization should succeed");

        assert_eq!(serialized, "42");
    }

    #[test]
    fn distinct_id_markers_can_share_numeric_values() {
        let job_id = JobId::from(7);
        let resource_group_id = ResourceGroupId::from(7);

        assert_eq!(job_id.get(), resource_group_id.get());
    }

    #[test]
    fn default_id_is_zero() {
        let job_id = JobId::default();

        assert_eq!(job_id.get(), 0);
    }
}
