//! Canonical transaction record types consumed by all export writers.
//!
//! These mirror the shapes from `harpoon-solana` but are self-contained
//! so that `harpoon-export` has no dependency on the Solana SDK.

use serde::Serialize;

/// A fully resolved transaction record ready for export.
#[derive(Debug, Clone, Serialize)]
pub struct TransactionRecord {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub signatures: Vec<String>,
    pub accounts: Vec<String>,
    pub program_ids: Vec<String>,
    pub fee: u64,
    pub account_balance_deltas: Vec<AccountBalanceDelta>,
    pub token_balance_deltas: Vec<TokenBalanceDelta>,
    pub log_messages: Option<Vec<String>>,
    pub err: Option<String>,
    pub instructions: Vec<InstructionRecord>,
}

/// SOL balance change for one account within a transaction.
#[derive(Debug, Clone, Serialize)]
pub struct AccountBalanceDelta {
    pub account: String,
    pub pre_lamports: u64,
    pub post_lamports: u64,
    pub delta_lamports: i64,
}

/// Token balance change for one (mint, owner, account_index) triple.
#[derive(Debug, Clone, Serialize)]
pub struct TokenBalanceDelta {
    pub mint: String,
    pub owner: String,
    pub account_index: u32,
    pub pre_ui_amount: Option<String>,
    pub post_ui_amount: Option<String>,
}

/// A single instruction with resolved account keys and base58-encoded data.
#[derive(Debug, Clone, Serialize)]
pub struct InstructionRecord {
    pub program_id: String,
    pub data: String,
    pub accounts: Vec<String>,
}
