//! Spider task input/output wire-format codec.
//!
//! Converts between a Rust value and the `MessagePack`-encoded
//! [`TaskInput::ValuePayload`] / [`TaskOutput`] payload Spider exchanges over a single job
//! input/output boundary.

use serde::Serialize;
use serde::de::DeserializeOwned;
use spider_core::types::io::TaskInput;
use spider_core::types::io::TaskOutput;

/// Encodes `value` as a `MessagePack` [`TaskInput::ValuePayload`].
///
/// # Type Parameters
///
/// * `T` - A serializable input value type.
///
/// # Returns
///
/// The msgpack-encoded [`TaskInput::ValuePayload`] on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`rmp_serde::to_vec`]'s return values on failure.
pub fn encode_input<T>(value: &T) -> anyhow::Result<TaskInput>
where
    T: Serialize, {
    Ok(TaskInput::ValuePayload(rmp_serde::to_vec(value)?))
}

/// Decodes a `MessagePack` [`TaskOutput`] payload into `T`.
///
/// # Type Parameters
///
/// * `T` - The deserialized output value type.
///
/// # Returns
///
/// The decoded `T` on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`rmp_serde::from_slice`]'s return values on failure.
pub fn decode_output<T>(output: &TaskOutput) -> anyhow::Result<T>
where
    T: DeserializeOwned, {
    Ok(rmp_serde::from_slice(output)?)
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde::Serialize;
    use serde::de::DeserializeOwned;
    use spider_core::types::io::TaskInput;
    use spider_core::types::io::TaskOutput;

    use super::decode_output;
    use super::encode_input;

    /// Round-trips `value` through [`encode_input`] then [`decode_output`].
    fn round_trip<T>(value: &T) -> T
    where
        T: Serialize + DeserializeOwned, {
        let encoded = encode_input(value).expect("encode_input should succeed");
        let TaskInput::ValuePayload(bytes) = encoded;
        decode_output(&bytes).expect("decode_output should succeed")
    }

    #[test]
    fn encode_input_wraps_msgpack_bytes_in_value_payload() {
        let value = 42.0_f64;
        let encoded = encode_input(&value).expect("encode_input should succeed");
        let expected = rmp_serde::to_vec(&value).expect("rmp_serde::to_vec should succeed");
        assert_eq!(encoded, TaskInput::ValuePayload(expected));
    }

    #[test]
    fn round_trip_preserves_floats() {
        let values = [
            0.0_f64,
            -0.0,
            1.5,
            -2.25,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
            f64::MIN,
            f64::MAX,
            f64::MIN_POSITIVE,
        ];
        for &value in &values {
            let got = round_trip(&value);
            assert_eq!(
                got.to_bits(),
                value.to_bits(),
                "float {value:?} not preserved"
            );
        }
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Sample {
        flag: bool,
        count: i64,
        label: String,
    }

    #[test]
    fn round_trip_preserves_struct() {
        let value = Sample {
            flag: true,
            count: -7,
            label: "hello".to_owned(),
        };
        assert_eq!(round_trip(&value), value);
    }

    #[test]
    fn round_trip_preserves_multiple_values_end_to_end() {
        let values: Vec<f64> = vec![1.5, -2.25, 3.0, 0.0, f64::INFINITY];
        let inputs: Vec<TaskInput> = values
            .iter()
            .map(encode_input)
            .collect::<anyhow::Result<Vec<_>>>()
            .expect("encoding all values should succeed");
        let outputs: Vec<TaskOutput> = inputs
            .into_iter()
            .map(|TaskInput::ValuePayload(bytes)| bytes)
            .collect();
        let decoded: Vec<f64> = outputs
            .iter()
            .map(decode_output)
            .collect::<anyhow::Result<Vec<_>>>()
            .expect("decoding all values should succeed");
        assert_eq!(
            decoded.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            values.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
        );
    }

    #[test]
    fn decode_output_errors_on_empty_payload() {
        let result = decode_output::<f64>(&TaskOutput::new());
        assert!(
            result.is_err(),
            "decoding an empty payload should fail, not panic"
        );
    }
}
