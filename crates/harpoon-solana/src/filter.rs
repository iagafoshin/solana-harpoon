use {
    solana_sdk::pubkey::Pubkey,
    solana_transaction_status::{InnerInstructions, TransactionStatusMeta},
};

/// Check whether a transaction touches any of `target_programs`, looking at
/// both outer (top-level) and inner (CPI) instructions.
///
/// `account_keys` must be the **full** key list — static keys plus any
/// loaded addresses resolved via [`resolve_full_account_keys`](crate::resolve_full_account_keys).
///
/// `meta` is optional; without it only outer instructions are checked.
pub fn matches_programs(
    target_programs: &[Pubkey],
    account_keys: &[Pubkey],
    outer_instructions: &[solana_sdk::instruction::CompiledInstruction],
    meta: Option<&TransactionStatusMeta>,
) -> bool {
    if matches_outer(target_programs, account_keys, outer_instructions) {
        return true;
    }

    let inner = meta.and_then(|m| m.inner_instructions.as_ref());
    matches_inner(target_programs, account_keys, inner)
}

fn program_matches(program_id_index: u8, account_keys: &[Pubkey], targets: &[Pubkey]) -> bool {
    account_keys
        .get(program_id_index as usize)
        .is_some_and(|key| targets.contains(key))
}

/// Quick check: is any target program present in the account keys at all?
///
/// If not, the transaction cannot possibly invoke the target program — not even
/// via CPI — because inner instruction `program_id_index` values reference the
/// same account-key array.
///
/// For v0 transactions the static keys may not include addresses loaded via
/// address-lookup tables, so this is a conservative fast-reject: a `false`
/// return means "definitely no match", but `true` means "maybe — check further".
pub fn could_match_programs(target_programs: &[Pubkey], account_keys: &[Pubkey]) -> bool {
    target_programs.iter().any(|t| account_keys.contains(t))
}

/// Check whether any **outer** (top-level) instruction invokes a target program.
pub fn matches_outer_programs(
    targets: &[Pubkey],
    account_keys: &[Pubkey],
    instructions: &[solana_sdk::instruction::CompiledInstruction],
) -> bool {
    matches_outer(targets, account_keys, instructions)
}

fn matches_outer(
    targets: &[Pubkey],
    account_keys: &[Pubkey],
    instructions: &[solana_sdk::instruction::CompiledInstruction],
) -> bool {
    instructions
        .iter()
        .any(|ix| program_matches(ix.program_id_index, account_keys, targets))
}

fn matches_inner(
    targets: &[Pubkey],
    account_keys: &[Pubkey],
    inner_instruction_sets: Option<&Vec<InnerInstructions>>,
) -> bool {
    let Some(inner_sets) = inner_instruction_sets else {
        return false;
    };
    for inner in inner_sets {
        for ix in &inner.instructions {
            if program_matches(ix.instruction.program_id_index, account_keys, targets) {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_sdk::{instruction::CompiledInstruction, pubkey::Pubkey},
    };

    fn make_keys(n: usize) -> Vec<Pubkey> {
        (0..n).map(|_| Pubkey::new_unique()).collect()
    }

    #[test]
    fn outer_match() {
        let keys = make_keys(3);
        let target = keys[2];
        let ix = CompiledInstruction {
            program_id_index: 2,
            accounts: vec![0, 1],
            data: vec![],
        };
        assert!(matches_programs(&[target], &keys, &[ix], None));
    }

    #[test]
    fn no_match_when_empty() {
        let keys = make_keys(3);
        let target = Pubkey::new_unique();
        let ix = CompiledInstruction {
            program_id_index: 0,
            accounts: vec![],
            data: vec![],
        };
        assert!(!matches_programs(&[target], &keys, &[ix], None));
    }
}
