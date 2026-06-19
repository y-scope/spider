//! Helpers for encoding and decoding binary protobuf payloads.

use std::io::Cursor;

use crate::{
    error::Error,
    storage::{BinaryPayload, BinaryPayloadEncoding},
};

const ZSTD_LEVEL: i32 = 0;

/// Encodes bytes as a raw binary payload.
///
/// # Returns
///
/// A [`BinaryPayload`] carrying `raw` without compression.
#[must_use]
pub const fn encode_payload_raw(raw: Vec<u8>) -> BinaryPayload {
    BinaryPayload {
        encoding: BinaryPayloadEncoding::Raw as i32,
        data: raw,
    }
}

/// Encodes bytes as a zstd-compressed binary payload.
///
/// # Returns
///
/// A [`BinaryPayload`] carrying zstd-compressed data on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`Error::BinaryPayloadCompression`] if zstd compression fails.
pub fn encode_payload_zstd(raw: Vec<u8>) -> Result<BinaryPayload, Error> {
    let data = encode_zstd_bytes(raw)?;
    Ok(BinaryPayload {
        encoding: BinaryPayloadEncoding::Zstd as i32,
        data,
    })
}

/// Encodes bytes with zstd.
///
/// # Returns
///
/// Zstd-compressed bytes on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`Error::BinaryPayloadCompression`] if zstd compression fails.
pub fn encode_zstd_bytes(raw: Vec<u8>) -> Result<Vec<u8>, Error> {
    zstd::stream::encode_all(Cursor::new(raw), ZSTD_LEVEL)
        .map_err(|e| Error::BinaryPayloadCompression(e.to_string()))
}

/// Decodes zstd-compressed bytes.
///
/// # Returns
///
/// Raw bytes on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`Error::BinaryPayloadDecompression`] if zstd decompression fails.
pub fn decode_zstd_bytes(compressed: Vec<u8>) -> Result<Vec<u8>, Error> {
    zstd::stream::decode_all(Cursor::new(compressed))
        .map_err(|e| Error::BinaryPayloadDecompression(e.to_string()))
}

/// Encodes bytes as zstd only when compression reduces the payload size.
///
/// # Returns
///
/// A [`BinaryPayload`] carrying the smaller of raw bytes and zstd-compressed bytes on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`encode_payload_zstd`]'s return values on failure.
pub fn encode_payload_adaptively(raw: Vec<u8>) -> Result<BinaryPayload, Error> {
    let compressed = encode_payload_zstd(raw.clone())?;
    if compressed.data.len() < raw.len() {
        Ok(compressed)
    } else {
        Ok(encode_payload_raw(raw))
    }
}

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
        BinaryPayloadEncoding::Zstd => decode_zstd_bytes(payload.data),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        payload::{
            decode_payload,
            decode_zstd_bytes,
            encode_payload_adaptively,
            encode_payload_zstd,
            encode_zstd_bytes,
        },
        storage::{BinaryPayload, BinaryPayloadEncoding},
    };

    #[test]
    fn zstd_payload_round_trips() {
        let raw = vec![42u8; 4096];

        let payload = encode_payload_zstd(raw.clone()).expect("zstd encoding should succeed");
        assert_eq!(payload.encoding, BinaryPayloadEncoding::Zstd as i32);
        assert!(
            payload.data.len() < raw.len(),
            "zstd payload should be smaller for repeated bytes"
        );

        let decoded = decode_payload(payload).expect("zstd decoding should succeed");
        assert_eq!(decoded, raw);
    }

    #[test]
    fn zstd_bytes_round_trip() {
        let raw = vec![42u8; 4096];

        let compressed = encode_zstd_bytes(raw.clone()).expect("zstd encoding should succeed");
        assert!(
            compressed.len() < raw.len(),
            "zstd bytes should be smaller for repeated bytes"
        );

        let decoded = decode_zstd_bytes(compressed).expect("zstd decoding should succeed");
        assert_eq!(decoded, raw);
    }

    #[test]
    fn adaptive_payload_keeps_raw_when_zstd_is_larger() {
        let raw = vec![1, 2, 3, 4];

        let payload =
            encode_payload_adaptively(raw.clone()).expect("adaptive encoding should succeed");

        assert_eq!(payload.encoding, BinaryPayloadEncoding::Raw as i32);
        assert_eq!(payload.data, raw);
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
