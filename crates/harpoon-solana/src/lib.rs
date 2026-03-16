mod decode;
mod extract;
mod filter;

pub use {
    decode::{decode_metadata, decode_metadata_reuse, decode_transaction},
    extract::{
        build_account_balance_deltas, build_token_balance_deltas, collect_program_ids,
        extract_instructions, resolve_full_account_keys, AccountBalanceDelta, ParsedInstruction,
        TokenBalanceDelta,
    },
    filter::matches_programs,
};

/// Errors produced by this crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to deserialize transaction: {0}")]
    TransactionDecode(#[source] bincode::Error),

    #[error("failed to decompress metadata (zstd): {0}")]
    MetadataDecompress(#[source] std::io::Error),

    #[error("failed to decode metadata (protobuf): {0}")]
    MetadataProtobuf(#[source] prost::DecodeError),

    #[error("failed to decode metadata (bincode): {0}")]
    MetadataBincode(#[source] bincode::Error),

    #[error("failed to convert protobuf metadata: {0}")]
    MetadataConvert(String),
}
