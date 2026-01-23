use non_empty_string::NonEmptyString;
use serde::{Deserialize, Serialize};

use crate::task::Error;

/// Type descriptor for all supported integer types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntTypeDescriptor {
    Int8,
    Int16,
    Int32,
    Int64,
}

/// Type descriptor for all supported floating-point types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FloatTypeDescriptor {
    Float32,
    Float64,
}

/// Type descriptor for all supported primitive types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrimitiveTypeDescriptor {
    Int(IntTypeDescriptor),
    Float(FloatTypeDescriptor),
    Boolean,
}

/// Type descriptor for the byte array type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BytesTypeDescriptor {}

/// Type descriptor for all supported types that can be used as a key in a map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MapKeyTypeDescriptor {
    Int(IntTypeDescriptor),
    Bytes(BytesTypeDescriptor),
}

/// Type descriptor for all supported types of a value.
///
/// # NOTE
///
/// * As a descriptor, it doesn't record the field information for `Struct`. Instead, only the name
///   is recorded as an identifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueTypeDescriptor {
    Primitive(PrimitiveTypeDescriptor),
    Bytes(BytesTypeDescriptor),
    Struct(NonEmptyString),
    List(Box<Self>),
    Map {
        key: MapKeyTypeDescriptor,
        value: Box<Self>,
    },
}

/// Type descriptor for all supported data types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataTypeDescriptor {
    Value(ValueTypeDescriptor),
    SharedValue(ValueTypeDescriptor),
}

impl DataTypeDescriptor {
    /// Serializes to JSON format.
    ///
    /// # Returns
    ///
    /// The serialized JSON string representation of the type descriptor on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`serde_json::to_string`]'s return values on failure.
    pub fn to_json(&self) -> Result<String, Error> {
        serde_json::to_string(self).map_err(Into::into)
    }

    /// Deserializes from a JSON string.
    ///
    /// # Returns
    ///
    /// The deserialized type descriptor on success.
    ///
    /// # Errors
    ///
    /// * Forwards [`serde_json::from_str`]'s return values on failure.
    pub fn from_json(json: &str) -> Result<Self, Error> {
        serde_json::from_str(json).map_err(Into::into)
    }

    /// Serializes to `MessagePack`.
    ///
    /// # Returns
    ///
    /// The serialized type descriptor in `MessagePack` in binary format on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`rmp_serde::to_vec`]'s return values on failure.
    /// * Forwards [`rmp_serde::to_vec_named`]'s return values on failure.
    pub fn to_msgpack(&self, with_named_fields: bool) -> Result<Vec<u8>, Error> {
        if with_named_fields {
            return rmp_serde::to_vec_named(self).map_err(Into::into);
        }
        rmp_serde::to_vec(self).map_err(Into::into)
    }

    /// Deserializes from `MessagePack`.
    ///
    /// # Returns
    ///
    /// The deserialized type descriptor on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`rmp_serde::from_slice`]'s return values on failure.
    pub fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        rmp_serde::from_slice(bytes).map_err(Into::into)
    }
}

impl ValueTypeDescriptor {
    #[must_use]
    pub const fn int8() -> Self {
        Self::Primitive(PrimitiveTypeDescriptor::Int(IntTypeDescriptor::Int8))
    }

    #[must_use]
    pub const fn int16() -> Self {
        Self::Primitive(PrimitiveTypeDescriptor::Int(IntTypeDescriptor::Int16))
    }

    #[must_use]
    pub const fn int32() -> Self {
        Self::Primitive(PrimitiveTypeDescriptor::Int(IntTypeDescriptor::Int32))
    }

    #[must_use]
    pub const fn int64() -> Self {
        Self::Primitive(PrimitiveTypeDescriptor::Int(IntTypeDescriptor::Int64))
    }

    #[must_use]
    pub const fn float32() -> Self {
        Self::Primitive(PrimitiveTypeDescriptor::Float(FloatTypeDescriptor::Float32))
    }

    #[must_use]
    pub const fn float64() -> Self {
        Self::Primitive(PrimitiveTypeDescriptor::Float(FloatTypeDescriptor::Float64))
    }

    #[must_use]
    pub const fn bool() -> Self {
        Self::Primitive(PrimitiveTypeDescriptor::Boolean)
    }

    #[must_use]
    pub const fn bytes() -> Self {
        Self::Bytes(BytesTypeDescriptor {})
    }

    /// Creates a `Struct` type descriptor from the given struct name.
    ///
    /// # Returns
    ///
    /// The [`ValueTypeDescriptor::Struct`] variant with the provided struct name.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidStructName`] if the provided struct name is empty.
    pub fn struct_from_name(name: impl Into<String>) -> Result<Self, Error> {
        Ok(Self::Struct(NonEmptyString::new(name.into()).map_err(
            |_| Error::InvalidStructName(EMPTY_STRUCT_NAME_ERROR.to_owned()),
        )?))
    }

    #[must_use]
    pub fn list(element_type: Self) -> Self {
        Self::List(Box::new(element_type))
    }

    #[must_use]
    pub fn map(key: MapKeyTypeDescriptor, value: Self) -> Self {
        Self::Map {
            key,
            value: Box::new(value),
        }
    }
}

const EMPTY_STRUCT_NAME_ERROR: &str = "Empty struct name is not allowed";

#[cfg(test)]
mod tests {
    use super::*;

    /// Asserts that the given descriptor can be serialized and deserialized to JSON and back to the
    /// original descriptor.
    fn assert_json_roundtrip(descriptor: &DataTypeDescriptor) {
        let json = descriptor
            .to_json()
            .expect("JSON serialization should succeed for valid descriptor");

        let deserialized = DataTypeDescriptor::from_json(&json)
            .expect("JSON deserialization should succeed for valid JSON");

        assert_eq!(
            *descriptor, deserialized,
            "Deserialized descriptor should match original"
        );
    }

    /// Asserts that the given descriptor can be serialized and deserialized to `MessagePack` and
    /// back to the original descriptor.
    fn assert_msgpack_roundtrip(descriptor: &DataTypeDescriptor, with_named_fields: bool) {
        let msgpack = descriptor
            .to_msgpack(with_named_fields)
            .expect("MessagePack serialization should succeed for valid descriptor");

        let deserialized = DataTypeDescriptor::from_msgpack(&msgpack)
            .expect("MessagePack deserialization should succeed for valid bytes");

        assert_eq!(
            *descriptor, deserialized,
            "Deserialized descriptor should match original (named_fields={with_named_fields})"
        );
    }

    /// Asserts that the given descriptor can be serialized and deserialized in all supported
    /// formats.
    fn assert_all_formats_roundtrip(descriptor: &DataTypeDescriptor) {
        assert_json_roundtrip(descriptor);
        assert_msgpack_roundtrip(descriptor, false);
        assert_msgpack_roundtrip(descriptor, true);
    }

    #[test]
    fn test_all_int_types() {
        let int_descriptors = vec![
            ValueTypeDescriptor::int8(),
            ValueTypeDescriptor::int16(),
            ValueTypeDescriptor::int32(),
            ValueTypeDescriptor::int64(),
        ];

        for value_descriptor in int_descriptors {
            assert_all_formats_roundtrip(&DataTypeDescriptor::Value(value_descriptor.clone()));
            assert_all_formats_roundtrip(&DataTypeDescriptor::SharedValue(value_descriptor));
        }
    }

    #[test]
    fn test_all_float_types() {
        let float_descriptors = vec![
            ValueTypeDescriptor::float32(),
            ValueTypeDescriptor::float64(),
        ];

        for value_descriptor in float_descriptors {
            assert_all_formats_roundtrip(&DataTypeDescriptor::Value(value_descriptor.clone()));
            assert_all_formats_roundtrip(&DataTypeDescriptor::SharedValue(value_descriptor));
        }
    }

    #[test]
    fn test_boolean_type() {
        let value_descriptor = ValueTypeDescriptor::bool();

        assert_all_formats_roundtrip(&DataTypeDescriptor::Value(value_descriptor.clone()));
        assert_all_formats_roundtrip(&DataTypeDescriptor::SharedValue(value_descriptor));
    }

    #[test]
    fn test_bytes_type() {
        let value_descriptor = ValueTypeDescriptor::bytes();

        assert_all_formats_roundtrip(&DataTypeDescriptor::Value(value_descriptor.clone()));
        assert_all_formats_roundtrip(&DataTypeDescriptor::SharedValue(value_descriptor));
    }

    #[test]
    fn test_struct_types() {
        let struct_names = vec![
            "User",
            "MyStruct",
            "user_profile",
            "HTTPRequest",
            "Struct123",
        ];

        for name in struct_names {
            let value_descriptor =
                ValueTypeDescriptor::struct_from_name(name).expect("should always be valid");
            assert_all_formats_roundtrip(&DataTypeDescriptor::Value(value_descriptor.clone()));
            assert_all_formats_roundtrip(&DataTypeDescriptor::SharedValue(value_descriptor));
        }
    }

    #[test]
    fn test_empty_struct_name() {
        let result = ValueTypeDescriptor::struct_from_name("");
        assert!(result.is_err());

        if let Err(Error::InvalidStructName(_)) = result {
        } else {
            panic!("Expected InvalidStructName error");
        }
    }

    #[test]
    fn test_list_of_primitives() {
        let element_types = vec![
            ValueTypeDescriptor::int8(),
            ValueTypeDescriptor::int16(),
            ValueTypeDescriptor::int32(),
            ValueTypeDescriptor::int64(),
            ValueTypeDescriptor::float32(),
            ValueTypeDescriptor::float64(),
            ValueTypeDescriptor::bool(),
        ];

        for element in element_types {
            let value_descriptor = ValueTypeDescriptor::list(element);
            assert_all_formats_roundtrip(&DataTypeDescriptor::Value(value_descriptor.clone()));
            assert_all_formats_roundtrip(&DataTypeDescriptor::SharedValue(value_descriptor));
        }
    }

    #[test]
    fn test_list_of_bytes() {
        let value_descriptor = ValueTypeDescriptor::list(ValueTypeDescriptor::bytes());
        assert_all_formats_roundtrip(&DataTypeDescriptor::Value(value_descriptor.clone()));
        assert_all_formats_roundtrip(&DataTypeDescriptor::SharedValue(value_descriptor));
    }

    #[test]
    fn test_list_of_structs() {
        let value_descriptor = ValueTypeDescriptor::list(
            ValueTypeDescriptor::struct_from_name("Product").expect("should always be valid"),
        );
        assert_all_formats_roundtrip(&DataTypeDescriptor::Value(value_descriptor.clone()));
        assert_all_formats_roundtrip(&DataTypeDescriptor::SharedValue(value_descriptor));
    }

    #[test]
    fn test_nested_lists() {
        // List<List<Int32>>
        let value_descriptor =
            ValueTypeDescriptor::list(ValueTypeDescriptor::list(ValueTypeDescriptor::int32()));
        assert_all_formats_roundtrip(&DataTypeDescriptor::Value(value_descriptor.clone()));
        assert_all_formats_roundtrip(&DataTypeDescriptor::SharedValue(value_descriptor));
    }

    #[test]
    fn test_deeply_nested_lists() {
        // List<List<List<Float64>>>
        let value_descriptor = ValueTypeDescriptor::list(ValueTypeDescriptor::list(
            ValueTypeDescriptor::list(ValueTypeDescriptor::float64()),
        ));
        assert_all_formats_roundtrip(&DataTypeDescriptor::Value(value_descriptor.clone()));
        assert_all_formats_roundtrip(&DataTypeDescriptor::SharedValue(value_descriptor));
    }

    #[test]
    fn test_map_with_int_keys() {
        let int_key_types = vec![
            IntTypeDescriptor::Int8,
            IntTypeDescriptor::Int16,
            IntTypeDescriptor::Int32,
            IntTypeDescriptor::Int64,
        ];

        for key_type in int_key_types {
            let value_descriptor = ValueTypeDescriptor::map(
                MapKeyTypeDescriptor::Int(key_type),
                ValueTypeDescriptor::struct_from_name("Value").unwrap(),
            );

            assert_all_formats_roundtrip(&DataTypeDescriptor::Value(value_descriptor.clone()));
            assert_all_formats_roundtrip(&DataTypeDescriptor::SharedValue(value_descriptor));
        }
    }

    #[test]
    fn test_map_with_string_key() {
        let value_descriptor = ValueTypeDescriptor::map(
            MapKeyTypeDescriptor::Bytes(BytesTypeDescriptor {}),
            ValueTypeDescriptor::int64(),
        );

        assert_all_formats_roundtrip(&DataTypeDescriptor::Value(value_descriptor.clone()));
        assert_all_formats_roundtrip(&DataTypeDescriptor::SharedValue(value_descriptor));
    }

    #[test]
    fn test_map_with_various_value_types() {
        let value_types = vec![
            ValueTypeDescriptor::bool(),
            ValueTypeDescriptor::float32(),
            ValueTypeDescriptor::bytes(),
            ValueTypeDescriptor::struct_from_name("Data").unwrap(),
            ValueTypeDescriptor::list(ValueTypeDescriptor::int32()),
        ];

        for value_type in value_types {
            let value_descriptor = ValueTypeDescriptor::map(
                MapKeyTypeDescriptor::Int(IntTypeDescriptor::Int32),
                value_type,
            );

            assert_all_formats_roundtrip(&DataTypeDescriptor::Value(value_descriptor.clone()));
            assert_all_formats_roundtrip(&DataTypeDescriptor::SharedValue(value_descriptor));
        }
    }

    #[test]
    fn test_complex_nesting() {
        // Map<Bytes, List<Map<Int32, List<Map<Int64, List<Map<Bytes, Bytes>>>>>>>
        let value_descriptor = ValueTypeDescriptor::map(
            MapKeyTypeDescriptor::Bytes(BytesTypeDescriptor {}),
            ValueTypeDescriptor::list(ValueTypeDescriptor::map(
                MapKeyTypeDescriptor::Int(IntTypeDescriptor::Int32),
                ValueTypeDescriptor::list(ValueTypeDescriptor::map(
                    MapKeyTypeDescriptor::Int(IntTypeDescriptor::Int64),
                    ValueTypeDescriptor::list(ValueTypeDescriptor::map(
                        MapKeyTypeDescriptor::Bytes(BytesTypeDescriptor {}),
                        ValueTypeDescriptor::bytes(),
                    )),
                )),
            )),
        );

        assert_all_formats_roundtrip(&DataTypeDescriptor::Value(value_descriptor.clone()));
        assert_all_formats_roundtrip(&DataTypeDescriptor::SharedValue(value_descriptor));
    }
}
