//! Read Parquet files back into [`TransactionRecord`]s.

use {
    crate::{
        record::{
            AccountBalanceDelta, InstructionRecord, TokenBalanceDelta, TransactionRecord,
        },
        Error,
    },
    arrow::array::{
        Array, AsArray, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
        UInt32Array, UInt64Array,
    },
    parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    std::path::Path,
};

/// Read all [`TransactionRecord`]s from a Parquet file.
pub fn read_parquet_records(path: &Path) -> Result<ParquetRecordIterator, Error> {
    let file = std::fs::File::open(path).map_err(Error::Io)?;
    let reader = ParquetRecordBatchReader::try_new(file, 4096)
        .map_err(|e| Error::Parquet(e.to_string()))?;
    Ok(ParquetRecordIterator {
        inner: reader,
        current_batch: Vec::new(),
        batch_pos: 0,
    })
}

/// Streaming iterator over [`TransactionRecord`]s from a Parquet file.
pub struct ParquetRecordIterator {
    inner: ParquetRecordBatchReader,
    current_batch: Vec<TransactionRecord>,
    batch_pos: usize,
}

impl Iterator for ParquetRecordIterator {
    type Item = Result<TransactionRecord, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.batch_pos < self.current_batch.len() {
                let record = self.current_batch[self.batch_pos].clone();
                self.batch_pos += 1;
                return Some(Ok(record));
            }
            // Need next batch
            match self.inner.next() {
                Some(Ok(batch)) => match batch_to_records(&batch) {
                    Ok(records) => {
                        self.current_batch = records;
                        self.batch_pos = 0;
                    }
                    Err(e) => return Some(Err(e)),
                },
                Some(Err(e)) => return Some(Err(Error::Parquet(e.to_string()))),
                None => return None,
            }
        }
    }
}

/// Convert an Arrow [`RecordBatch`] into [`TransactionRecord`]s.
fn batch_to_records(batch: &RecordBatch) -> Result<Vec<TransactionRecord>, Error> {
    let rows = batch.num_rows();
    let mut records = Vec::with_capacity(rows);

    let slot_arr = batch.column_by_name("slot")
        .expect("slot column")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("slot is UInt64");

    let block_time_arr = batch.column_by_name("block_time")
        .expect("block_time column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("block_time is Int64");

    let sigs_arr = batch.column_by_name("signatures")
        .expect("signatures column")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("signatures is List");

    let accounts_arr = batch.column_by_name("accounts")
        .expect("accounts column")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("accounts is List");

    let program_ids_arr = batch.column_by_name("program_ids")
        .expect("program_ids column")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("program_ids is List");

    let fee_arr = batch.column_by_name("fee")
        .expect("fee column")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("fee is UInt64");

    let ab_arr = batch.column_by_name("account_balance_deltas")
        .expect("account_balance_deltas column")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("account_balance_deltas is List");

    let tb_arr = batch.column_by_name("token_balance_deltas")
        .expect("token_balance_deltas column")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("token_balance_deltas is List");

    let logs_arr = batch.column_by_name("log_messages")
        .expect("log_messages column")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("log_messages is List");

    let err_arr = batch.column_by_name("err")
        .expect("err column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("err is Utf8");

    let ix_arr = batch.column_by_name("instructions")
        .expect("instructions column")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("instructions is List");

    for row in 0..rows {
        let slot = slot_arr.value(row);
        let block_time = if block_time_arr.is_null(row) {
            None
        } else {
            Some(block_time_arr.value(row))
        };
        let signatures = extract_string_list(sigs_arr, row);
        let accounts = extract_string_list(accounts_arr, row);
        let program_ids = extract_string_list(program_ids_arr, row);
        let fee = fee_arr.value(row);
        let account_balance_deltas = extract_account_balance_deltas(ab_arr, row);
        let token_balance_deltas = extract_token_balance_deltas(tb_arr, row);
        let log_messages = if logs_arr.is_null(row) {
            None
        } else {
            Some(extract_string_list(logs_arr, row))
        };
        let err = if err_arr.is_null(row) {
            None
        } else {
            Some(err_arr.value(row).to_string())
        };
        let instructions = extract_instructions(ix_arr, row);

        records.push(TransactionRecord {
            slot,
            block_time,
            signatures,
            accounts,
            program_ids,
            fee,
            account_balance_deltas,
            token_balance_deltas,
            log_messages,
            err,
            instructions,
        });
    }

    Ok(records)
}

fn extract_string_list(arr: &ListArray, row: usize) -> Vec<String> {
    let values = arr.value(row);
    let str_arr = values.as_string::<i32>();
    (0..str_arr.len())
        .map(|i| str_arr.value(i).to_string())
        .collect()
}

fn extract_account_balance_deltas(arr: &ListArray, row: usize) -> Vec<AccountBalanceDelta> {
    let values = arr.value(row);
    let struct_arr = values
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("account_balance_deltas items are Struct");

    let account_col = struct_arr.column(0).as_string::<i32>();
    let pre_col = struct_arr.column(1).as_any().downcast_ref::<UInt64Array>().unwrap();
    let post_col = struct_arr.column(2).as_any().downcast_ref::<UInt64Array>().unwrap();
    let delta_col = struct_arr.column(3).as_any().downcast_ref::<Int64Array>().unwrap();

    (0..struct_arr.len())
        .map(|i| AccountBalanceDelta {
            account: account_col.value(i).to_string(),
            pre_lamports: pre_col.value(i),
            post_lamports: post_col.value(i),
            delta_lamports: delta_col.value(i),
        })
        .collect()
}

fn extract_token_balance_deltas(arr: &ListArray, row: usize) -> Vec<TokenBalanceDelta> {
    let values = arr.value(row);
    let struct_arr = values
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("token_balance_deltas items are Struct");

    let mint_col = struct_arr.column(0).as_string::<i32>();
    let owner_col = struct_arr.column(1).as_string::<i32>();
    let idx_col = struct_arr.column(2).as_any().downcast_ref::<UInt32Array>().unwrap();
    let pre_col = struct_arr.column(3).as_string::<i32>();
    let post_col = struct_arr.column(4).as_string::<i32>();

    (0..struct_arr.len())
        .map(|i| TokenBalanceDelta {
            mint: mint_col.value(i).to_string(),
            owner: owner_col.value(i).to_string(),
            account_index: idx_col.value(i),
            pre_ui_amount: if pre_col.is_null(i) {
                None
            } else {
                Some(pre_col.value(i).to_string())
            },
            post_ui_amount: if post_col.is_null(i) {
                None
            } else {
                Some(post_col.value(i).to_string())
            },
        })
        .collect()
}

fn extract_instructions(arr: &ListArray, row: usize) -> Vec<InstructionRecord> {
    let values = arr.value(row);
    let struct_arr = values
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("instructions items are Struct");

    let pid_col = struct_arr.column(0).as_string::<i32>();
    let data_col = struct_arr.column(1).as_string::<i32>();
    let accts_col = struct_arr
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("accounts is List");

    (0..struct_arr.len())
        .map(|i| {
            let accounts_val = accts_col.value(i);
            let accts_str = accounts_val.as_string::<i32>();
            let accounts: Vec<String> = (0..accts_str.len())
                .map(|j| accts_str.value(j).to_string())
                .collect();
            InstructionRecord {
                program_id: pid_col.value(i).to_string(),
                data: data_col.value(i).to_string(),
                accounts,
            }
        })
        .collect()
}
