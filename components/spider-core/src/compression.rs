//! Helpers for compressing and decompressing binary data.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to compress zstd bytes: {0}")]
    ZstdCompression(std::io::Error),

    #[error("failed to decompress zstd bytes: {0}")]
    ZstdDecompression(std::io::Error),
}

/// Encodes bytes with zstd.
///
/// # Errors
///
/// Returns [`Error::ZstdCompression`] if zstd compression fails.
pub fn encode_zstd_bytes(raw: &[u8]) -> Result<Vec<u8>, Error> {
    const ZSTD_LEVEL: i32 = 0;
    zstd::stream::encode_all(raw, ZSTD_LEVEL).map_err(Error::ZstdCompression)
}

/// Decodes zstd-compressed bytes.
///
/// # Errors
///
/// Returns [`Error::ZstdDecompression`] if zstd decompression fails.
pub fn decode_zstd_bytes(compressed: &[u8]) -> Result<Vec<u8>, Error> {
    zstd::stream::decode_all(compressed).map_err(Error::ZstdDecompression)
}

#[cfg(test)]
mod tests {
    use super::decode_zstd_bytes;
    use super::encode_zstd_bytes;

    #[test]
    fn zstd_bytes_round_trip() {
        let raw = vec![42u8; 4096];

        let compressed = encode_zstd_bytes(&raw).expect("zstd compression should succeed");
        assert!(
            compressed.len() < raw.len(),
            "zstd payload should be smaller for repeated bytes"
        );

        let decoded = decode_zstd_bytes(&compressed).expect("zstd decompression should succeed");

        assert_eq!(decoded, raw);
    }

    #[test]
    fn invalid_zstd_bytes_are_rejected() {
        let error = decode_zstd_bytes(b"not a zstd payload")
            .expect_err("invalid zstd payload should fail to decompress");

        assert!(error.to_string().contains("zstd"));
    }
}
