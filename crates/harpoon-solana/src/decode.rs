use {
    crate::Error,
    prost::Message,
    solana_sdk::transaction::VersionedTransaction,
    solana_storage_proto::{convert::generated, StoredTransactionStatusMeta},
    solana_transaction_status::TransactionStatusMeta,
    std::io::Read,
};

/// Deserialize a [`VersionedTransaction`] from raw bincode bytes (as stored in CAR).
pub fn decode_transaction(data: &[u8]) -> Result<VersionedTransaction, Error> {
    bincode::deserialize(data).map_err(Error::TransactionDecode)
}

/// Decode transaction metadata from zstd-compressed bytes.
///
/// Tries protobuf first, falls back to bincode (both formats appear in the wild).
/// Returns `Ok(None)` when the input (or decompressed payload) is empty.
pub fn decode_metadata(compressed: &[u8]) -> Result<Option<TransactionStatusMeta>, Error> {
    if compressed.is_empty() {
        return Ok(None);
    }

    let buffer = zstd::decode_all(compressed).map_err(Error::MetadataDecompress)?;
    if buffer.is_empty() {
        return Ok(None);
    }

    decode_transaction_status_meta(&buffer).map(Some)
}

/// Decode metadata using a caller-provided decompressor and buffer to reduce allocations.
///
/// The `decompressor` reuses the zstd DCtx across calls (the expensive internal state).
/// The `buf` is used as a fallback when the bulk decompressor's capacity estimate is wrong.
/// Both should be stored in a `thread_local!` for maximum reuse on rayon thread pools.
pub fn decode_metadata_reuse(
    compressed: &[u8],
    decompressor: &mut zstd::bulk::Decompressor<'_>,
    buf: &mut Vec<u8>,
) -> Result<Option<TransactionStatusMeta>, Error> {
    if compressed.is_empty() {
        return Ok(None);
    }

    // Try bulk decompressor first — reuses DCtx, estimates capacity at 4x compressed size.
    let buffer = match decompressor.decompress(compressed, compressed.len().saturating_mul(4).max(256))
    {
        Ok(data) => data,
        Err(_) => {
            // Fallback: streaming decode into caller-provided buffer.
            buf.clear();
            let mut decoder = zstd::stream::Decoder::new(std::io::Cursor::new(compressed))
                .map_err(Error::MetadataDecompress)?;
            decoder.read_to_end(buf).map_err(Error::MetadataDecompress)?;
            return if buf.is_empty() {
                Ok(None)
            } else {
                decode_transaction_status_meta(buf).map(Some)
            };
        }
    };

    if buffer.is_empty() {
        return Ok(None);
    }

    decode_transaction_status_meta(&buffer).map(Some)
}

/// Decode from raw (already-decompressed) bytes. Protobuf first, bincode fallback.
fn decode_transaction_status_meta(buffer: &[u8]) -> Result<TransactionStatusMeta, Error> {
    match generated::TransactionStatusMeta::decode(buffer) {
        Ok(proto) => TransactionStatusMeta::try_from(proto)
            .map_err(|e| Error::MetadataConvert(format!("{e}"))),
        Err(_) => {
            let stored: StoredTransactionStatusMeta =
                bincode::deserialize(buffer).map_err(Error::MetadataBincode)?;
            Ok(TransactionStatusMeta::from(stored))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_empty_metadata_returns_none() {
        assert!(decode_metadata(&[]).unwrap().is_none());
    }

    #[test]
    fn decode_transaction_rejects_garbage() {
        assert!(decode_transaction(&[0xDE, 0xAD]).is_err());
    }
}
