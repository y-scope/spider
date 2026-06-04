//! Helpers for converting Spider IDs to protobuf fields.

use spider_core::types::id::Id;

/// Converts a Spider UUID-backed ID into the protobuf byte representation.
///
/// # Returns
///
/// The UUID bytes for `id`.
#[must_use]
pub fn id_to_bytes<TypeMarker: std::fmt::Debug + PartialEq + Eq>(id: &Id<TypeMarker>) -> Vec<u8> {
    id.as_bytes().to_vec()
}
