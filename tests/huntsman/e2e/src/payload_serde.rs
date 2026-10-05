//! Spider task output wire-format codec.
//!
//! Converts the `MessagePack`-encoded [`TaskOutput`] payload Spider exchanges over a single job
//! output boundary into a Rust value.

use serde::de::DeserializeOwned;
use spider_core::types::io::TaskOutput;

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
pub fn decode_output<T: DeserializeOwned>(output: &TaskOutput) -> anyhow::Result<T> {
    Ok(rmp_serde::from_slice(output)?)
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde::Serialize;
    use serde::de::DeserializeOwned;
    use spider_core::types::io::TaskGraphInputBuilder;
    use spider_core::types::io::TaskOutput;

    use super::decode_output;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Sample {
        flag: bool,
        count: i64,
        label: String,
    }

    /// Round-trips `value` through [`TaskGraphInputBuilder::append_task_input`] then
    /// [`decode_output`].
    ///
    /// # Type Parameters
    ///
    /// * `ValueType` - The type of the value to round-trip.
    ///
    /// # Returns
    ///
    /// The decoded value.
    ///
    /// # Panics
    ///
    /// Panics if `value` fails to be encoded or decoded, or if the built task graph input doesn't
    /// hold exactly one positional input.
    fn round_trip<ValueType: Serialize + DeserializeOwned>(value: &ValueType) -> ValueType {
        let mut builder = TaskGraphInputBuilder::new();
        builder
            .append_task_input(value)
            .expect("appending a task input should succeed");
        let [payload] =
            <[TaskOutput; 1]>::try_from(builder.build().into_positional_inputs(|payload| payload))
                .expect("the task graph input should hold exactly one positional input");
        decode_output(&payload).expect("decode_output should succeed")
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
    fn round_trip_preserves_multiple_values_end_to_end() -> anyhow::Result<()> {
        let values: Vec<f64> = vec![1.5, -2.25, 3.0, 0.0, f64::INFINITY];
        let mut builder = TaskGraphInputBuilder::new();
        for value in &values {
            builder.append_task_input(value)?;
        }
        let outputs: Vec<TaskOutput> = builder.build().into_positional_inputs(|payload| payload);
        let decoded: Vec<f64> = outputs
            .iter()
            .map(decode_output)
            .collect::<anyhow::Result<Vec<_>>>()?;
        assert_eq!(
            decoded.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            values.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
        );
        Ok(())
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
