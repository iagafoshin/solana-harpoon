use {
    serde::Serialize,
    solana_sdk::{
        instruction::CompiledInstruction, message::VersionedMessage, pubkey::Pubkey,
        transaction::VersionedTransaction,
    },
    solana_transaction_status::{TransactionStatusMeta, TransactionTokenBalance},
    std::{borrow::Cow, collections::BTreeMap},
};

/// A single instruction with resolved account keys and base58-encoded data.
#[derive(Debug, Clone, Serialize)]
pub struct ParsedInstruction {
    pub program_id: String,
    pub data: String,
    pub accounts: Vec<String>,
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

/// Build the full list of account keys for a transaction,
/// including any addresses loaded via address lookup tables (v0 transactions).
///
/// For legacy transactions or when `meta` is `None`, this is just the static keys.
pub fn resolve_full_account_keys(
    tx: &VersionedTransaction,
    meta: Option<&TransactionStatusMeta>,
) -> Vec<Pubkey> {
    let static_keys = match &tx.message {
        VersionedMessage::Legacy(msg) => &msg.account_keys,
        VersionedMessage::V0(msg) => &msg.account_keys,
    };

    let mut keys: Vec<Pubkey> = static_keys.clone();

    if let (VersionedMessage::V0(_), Some(meta)) = (&tx.message, meta) {
        keys.extend(meta.loaded_addresses.writable.iter().copied());
        keys.extend(meta.loaded_addresses.readonly.iter().copied());
    }

    keys
}

/// Extract all instructions (top-level + CPI inner instructions) with resolved
/// program IDs, account addresses (as base58 strings), and base58-encoded data.
pub fn extract_instructions(
    account_keys: &[Pubkey],
    tx: &VersionedTransaction,
    meta: Option<&TransactionStatusMeta>,
) -> Vec<ParsedInstruction> {
    let outer = match &tx.message {
        VersionedMessage::Legacy(msg) => &msg.instructions,
        VersionedMessage::V0(msg) => &msg.instructions,
    };

    let inner_sets = meta.and_then(|m| m.inner_instructions.as_ref());

    let capacity = outer.len()
        + inner_sets
            .map(|sets| sets.iter().map(|s| s.instructions.len()).sum())
            .unwrap_or(0);
    let mut result = Vec::with_capacity(capacity);

    for ix in outer {
        result.push(resolve_instruction(ix, account_keys));
    }

    if let Some(inner_sets) = inner_sets {
        for inner in inner_sets {
            for ix in &inner.instructions {
                result.push(resolve_instruction(&ix.instruction, account_keys));
            }
        }
    }

    result
}

/// Collect the deduplicated set of program IDs invoked in a transaction
/// (outer + inner instructions), sorted for deterministic output.
pub fn collect_program_ids(
    account_keys: &[Pubkey],
    tx: &VersionedTransaction,
    meta: Option<&TransactionStatusMeta>,
) -> Vec<Pubkey> {
    let outer = match &tx.message {
        VersionedMessage::Legacy(msg) => &msg.instructions,
        VersionedMessage::V0(msg) => &msg.instructions,
    };

    let inner_sets = meta.and_then(|m| m.inner_instructions.as_ref());

    let mut program_ids: Vec<Pubkey> = Vec::new();
    for ix in outer {
        if let Some(&key) = account_keys.get(ix.program_id_index as usize) {
            program_ids.push(key);
        }
    }
    if let Some(inner_sets) = inner_sets {
        for inner in inner_sets {
            for ix in &inner.instructions {
                if let Some(&key) = account_keys.get(ix.instruction.program_id_index as usize) {
                    program_ids.push(key);
                }
            }
        }
    }
    program_ids.sort_unstable();
    program_ids.dedup();
    program_ids
}

/// Build SOL balance deltas from transaction metadata.
pub fn build_account_balance_deltas(
    meta: &TransactionStatusMeta,
    account_keys: &[Pubkey],
) -> Vec<AccountBalanceDelta> {
    meta.pre_balances
        .iter()
        .zip(meta.post_balances.iter())
        .enumerate()
        .map(|(idx, (&pre, &post))| AccountBalanceDelta {
            account: account_keys
                .get(idx)
                .map(|key| key.to_string())
                .unwrap_or_else(|| format!("UNKNOWN_ACCOUNT_INDEX_{idx}")),
            pre_lamports: pre,
            post_lamports: post,
            delta_lamports: post as i64 - pre as i64,
        })
        .collect()
}

/// Build token balance deltas from transaction metadata.
pub fn build_token_balance_deltas(meta: &TransactionStatusMeta) -> Vec<TokenBalanceDelta> {
    let mut map: BTreeMap<(String, String, u8), TokenBalanceDelta> = BTreeMap::new();

    if let Some(pre) = meta.pre_token_balances.as_ref() {
        apply_token_balances(&mut map, pre.iter(), false);
    }
    if let Some(post) = meta.post_token_balances.as_ref() {
        apply_token_balances(&mut map, post.iter(), true);
    }

    map.into_values().collect()
}

// --- private helpers ---

fn resolve_account(account_keys: &[Pubkey], index: u8) -> String {
    account_keys
        .get(index as usize)
        .map(|key| key.to_string())
        .unwrap_or_else(|| format!("UNKNOWN_PROGRAM_INDEX_{index}"))
}

fn resolve_instruction(ix: &CompiledInstruction, account_keys: &[Pubkey]) -> ParsedInstruction {
    ParsedInstruction {
        program_id: resolve_account(account_keys, ix.program_id_index),
        data: bs58::encode(&ix.data).into_string(),
        accounts: ix
            .accounts
            .iter()
            .map(|&idx| resolve_account(account_keys, idx))
            .collect(),
    }
}

trait OwnerString {
    fn owner_string(&self) -> Cow<'_, str>;
}

impl OwnerString for String {
    fn owner_string(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.as_str())
    }
}

impl OwnerString for Option<String> {
    fn owner_string(&self) -> Cow<'_, str> {
        match self.as_deref() {
            Some(s) => Cow::Borrowed(s),
            None => Cow::Borrowed(""),
        }
    }
}

fn apply_token_balances<'a>(
    map: &mut BTreeMap<(String, String, u8), TokenBalanceDelta>,
    balances: impl Iterator<Item = &'a TransactionTokenBalance>,
    is_post: bool,
) {
    for balance in balances {
        let key = (
            balance.mint.clone(),
            balance.owner.owner_string().into_owned(),
            balance.account_index,
        );
        let entry = map.entry(key.clone()).or_insert_with(|| TokenBalanceDelta {
            mint: key.0.clone(),
            owner: key.1.clone(),
            account_index: u32::from(key.2),
            pre_ui_amount: None,
            post_ui_amount: None,
        });
        let amount = Some(balance.ui_token_amount.ui_amount_string.clone());
        if is_post {
            entry.post_ui_amount = amount;
        } else {
            entry.pre_ui_amount = amount;
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_sdk::{
            hash::Hash, message::Message, signature::Signature, transaction::Transaction,
        },
        solana_system_interface::instruction as system_instruction,
    };

    #[test]
    fn resolve_keys_legacy() {
        let from = Pubkey::new_unique();
        let to = Pubkey::new_unique();
        let ix = system_instruction::transfer(&from, &to, 1_000);
        let msg = Message::new(&[ix], Some(&from));
        let tx = Transaction {
            signatures: vec![Signature::default()],
            message: msg,
        };
        let vtx: VersionedTransaction = tx.into();
        let keys = resolve_full_account_keys(&vtx, None);
        // from, to, system_program
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&from));
        assert!(keys.contains(&to));
    }

    #[test]
    fn extract_instructions_basic() {
        let from = Pubkey::new_unique();
        let to = Pubkey::new_unique();
        let ix = system_instruction::transfer(&from, &to, 42);
        let msg = Message::new_with_blockhash(&[ix], Some(&from), &Hash::default());
        let tx = Transaction {
            signatures: vec![Signature::default()],
            message: msg,
        };
        let vtx: VersionedTransaction = tx.into();
        let keys = resolve_full_account_keys(&vtx, None);
        let instructions = extract_instructions(&keys, &vtx, None);
        assert_eq!(instructions.len(), 1);
        assert_eq!(
            instructions[0].program_id,
            solana_system_interface::program::id().to_string()
        );
        assert_eq!(instructions[0].accounts.len(), 2);
    }
}
