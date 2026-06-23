//! Helpers for encoding and decoding binary protobuf payloads.

use spider_core::compression::decode_zstd_bytes;

use crate::{
    error::Error,
    storage::{BinaryPayload, BinaryPayloadEncoding},
};

/// Decodes a binary payload into raw bytes.
///
/// # Returns
///
/// The decoded raw bytes on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`Error::BinaryPayloadEncodingUnknown`] if the encoding value is unknown.
/// * [`Error::BinaryPayloadEncodingUnspecified`] if the encoding value is unspecified.
/// * [`Error::BinaryPayloadDecompression`] if zstd decompression fails.
pub fn decode_payload(payload: BinaryPayload) -> Result<Vec<u8>, Error> {
    match BinaryPayloadEncoding::try_from(payload.encoding)
        .map_err(|_| Error::BinaryPayloadEncodingUnknown(payload.encoding))?
    {
        BinaryPayloadEncoding::Unspecified => Err(Error::BinaryPayloadEncodingUnspecified),
        BinaryPayloadEncoding::Raw => Ok(payload.data),
        BinaryPayloadEncoding::Zstd => decode_zstd_bytes(&payload.data)
            .map_err(|e| Error::BinaryPayloadDecompression(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use spider_core::compression::encode_zstd_bytes;

    use crate::{
        payload::decode_payload,
        storage::{BinaryPayload, BinaryPayloadEncoding},
    };

    #[test]
    fn zstd_payload_round_trips() {
        let raw = vec![42u8; 4096];

        let payload = BinaryPayload {
            encoding: BinaryPayloadEncoding::Zstd as i32,
            data: encode_zstd_bytes(&raw).expect("zstd encoding should succeed"),
        };
        assert_eq!(payload.encoding, BinaryPayloadEncoding::Zstd as i32);
        assert!(
            payload.data.len() < raw.len(),
            "zstd payload should be smaller for repeated bytes"
        );

        let decoded = decode_payload(payload).expect("zstd decoding should succeed");
        assert_eq!(decoded, raw);
    }

    #[test]
    fn raw_payload_decodes_without_compression() {
        let raw = vec![1, 2, 3, 4];
        let payload = BinaryPayload {
            encoding: BinaryPayloadEncoding::Raw as i32,
            data: raw.clone(),
        };

        let decoded = decode_payload(payload).expect("raw decoding should succeed");

        assert_eq!(decoded, raw);
    }
}
