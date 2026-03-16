pub mod record;
pub mod schema;
pub mod writer;

pub use {
    arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, SchemaRef as ArrowSchemaRef},
    record::{
        AccountBalanceDelta, FlatRecord, InstructionRecord, TokenBalanceDelta, TransactionRecord,
    },
    schema::{build_flat_schema, build_schema, flat_records_to_batch, records_to_batch},
    writer::{
        create_writer, CsvExportWriter, ExportWriter, FlatPartitionedWriter,
        JsonLinesExportWriter, OutputFormat, ParquetExportWriter, PartitionedWriter,
    },
};

/// Errors produced by this crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[source] std::io::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[source] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(String),

    #[error("JSON serialization error: {0}")]
    Json(#[source] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::{
            fs,
            io::{BufRead, BufReader},
        },
    };

    fn sample_records() -> Vec<TransactionRecord> {
        vec![
            TransactionRecord {
                slot: 100,
                block_time: Some(1_700_000_000),
                signatures: vec!["sig1abc".to_string()],
                accounts: vec!["acct1".to_string(), "acct2".to_string()],
                program_ids: vec!["prog1".to_string()],
                fee: 5000,
                account_balance_deltas: vec![AccountBalanceDelta {
                    account: "acct1".to_string(),
                    pre_lamports: 1_000_000,
                    post_lamports: 995_000,
                    delta_lamports: -5000,
                }],
                token_balance_deltas: vec![TokenBalanceDelta {
                    mint: "mintABC".to_string(),
                    owner: "ownerXYZ".to_string(),
                    account_index: 2,
                    pre_ui_amount: Some("100.5".to_string()),
                    post_ui_amount: Some("90.5".to_string()),
                }],
                log_messages: Some(vec!["Program prog1 invoke [1]".to_string()]),
                err: None,
                instructions: vec![InstructionRecord {
                    program_id: "prog1".to_string(),
                    data: "3Bxs4h24hBtQ".to_string(),
                    accounts: vec!["acct1".to_string(), "acct2".to_string()],
                }],
            },
            TransactionRecord {
                slot: 101,
                block_time: None,
                signatures: vec!["sig2def".to_string(), "sig2ghi".to_string()],
                accounts: vec!["acct3".to_string()],
                program_ids: vec!["prog2".to_string()],
                fee: 10_000,
                account_balance_deltas: vec![],
                token_balance_deltas: vec![],
                log_messages: None,
                err: Some("InstructionError".to_string()),
                instructions: vec![],
            },
        ]
    }

    // -----------------------------------------------------------------------
    // Schema & RecordBatch
    // -----------------------------------------------------------------------

    #[test]
    fn build_schema_has_expected_fields() {
        let schema = build_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            &[
                "slot",
                "block_time",
                "signatures",
                "accounts",
                "program_ids",
                "fee",
                "account_balance_deltas",
                "token_balance_deltas",
                "log_messages",
                "err",
                "instructions",
            ]
        );
    }

    #[test]
    fn records_to_batch_roundtrip() {
        let schema = build_schema();
        let records = sample_records();
        let batch = records_to_batch(&schema, &records).expect("batch");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 11);
    }

    #[test]
    fn empty_records_produce_empty_batch() {
        let schema = build_schema();
        let batch = records_to_batch(&schema, &[]).expect("batch");
        assert_eq!(batch.num_rows(), 0);
    }

    // -----------------------------------------------------------------------
    // Parquet writer
    // -----------------------------------------------------------------------

    #[test]
    fn parquet_write_and_read_back() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.parquet");

        let records = sample_records();
        let mut writer = Box::new(ParquetExportWriter::new(&path, 1024).expect("writer"));
        writer.write_records(&records).expect("write");
        writer.finish().expect("finish");

        // Read back and verify row count
        let file = fs::File::open(&path).expect("open");
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(file, 1024)
                .expect("reader");
        let batches: Vec<_> = reader.into_iter().collect::<Result<_, _>>().expect("read");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    // -----------------------------------------------------------------------
    // JSON Lines writer
    // -----------------------------------------------------------------------

    #[test]
    fn jsonl_write_produces_valid_lines() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.jsonl");

        let records = sample_records();
        let mut writer = Box::new(JsonLinesExportWriter::new(&path).expect("writer"));
        writer.write_records(&records).expect("write");
        writer.finish().expect("finish");

        let file = fs::File::open(&path).expect("open");
        let reader = BufReader::new(file);
        let lines: Vec<String> = reader.lines().collect::<Result<_, _>>().expect("read");
        assert_eq!(lines.len(), 2);

        // Each line should be valid JSON
        let v0: serde_json::Value = serde_json::from_str(&lines[0]).expect("parse line 0");
        assert_eq!(v0["slot"], 100);
        assert_eq!(v0["fee"], 5000);

        let v1: serde_json::Value = serde_json::from_str(&lines[1]).expect("parse line 1");
        assert_eq!(v1["slot"], 101);
        assert_eq!(v1["err"], "InstructionError");
        assert!(v1["block_time"].is_null());
    }

    // -----------------------------------------------------------------------
    // CSV writer
    // -----------------------------------------------------------------------

    #[test]
    fn csv_write_has_header_and_rows() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.csv");

        let records = sample_records();
        let mut writer = Box::new(CsvExportWriter::new(&path).expect("writer"));
        writer.write_records(&records).expect("write");
        writer.finish().expect("finish");

        let content = fs::read_to_string(&path).expect("read");
        let lines: Vec<&str> = content.lines().collect();
        // header + 2 data rows
        assert_eq!(lines.len(), 3);
        assert!(lines[0].starts_with("slot,"));
        assert!(lines[1].starts_with("100,"));
        assert!(lines[2].starts_with("101,"));
    }

    // -----------------------------------------------------------------------
    // create_writer factory
    // -----------------------------------------------------------------------

    #[test]
    fn create_writer_factory() {
        let dir = tempfile::tempdir().expect("tmpdir");

        for (fmt, ext) in [
            (OutputFormat::Parquet, "parquet"),
            (OutputFormat::Csv, "csv"),
            (OutputFormat::JsonLines, "jsonl"),
        ] {
            let path = dir.path().join(format!("out.{ext}"));
            let mut w = create_writer(&path, fmt, 1024).expect("create");
            w.write_records(&sample_records()).expect("write");
            w.finish().expect("finish");
            assert!(path.exists(), "file should exist: {path:?}");
            assert!(fs::metadata(&path).expect("meta").len() > 0);
        }
    }

    // -----------------------------------------------------------------------
    // Partitioned writer
    // -----------------------------------------------------------------------

    #[test]
    fn partitioned_writer_creates_separate_files() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let out_dir = dir.path().join("partitioned");

        let records = sample_records();
        let mut pw =
            PartitionedWriter::new(&out_dir, OutputFormat::JsonLines, 1024).expect("pw");

        // Partition by program_ids[0]
        pw.write_partitioned(&records, |r| {
            r.program_ids.first().cloned().unwrap_or_default()
        })
        .expect("write");
        pw.finish().expect("finish");

        // Should have two files: prog1.jsonl and prog2.jsonl
        let prog1 = out_dir.join("prog1.jsonl");
        let prog2 = out_dir.join("prog2.jsonl");
        assert!(prog1.exists(), "prog1.jsonl should exist");
        assert!(prog2.exists(), "prog2.jsonl should exist");

        let lines1: Vec<String> = BufReader::new(fs::File::open(&prog1).expect("open"))
            .lines()
            .collect::<Result<_, _>>()
            .expect("read");
        assert_eq!(lines1.len(), 1);

        let lines2: Vec<String> = BufReader::new(fs::File::open(&prog2).expect("open"))
            .lines()
            .collect::<Result<_, _>>()
            .expect("read");
        assert_eq!(lines2.len(), 1);
    }

    #[test]
    fn partitioned_parquet_creates_separate_files() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let out_dir = dir.path().join("partitioned_pq");

        let records = sample_records();
        let mut pw =
            PartitionedWriter::new(&out_dir, OutputFormat::Parquet, 1024).expect("pw");

        pw.write_partitioned(&records, |r| format!("slot_{}", r.slot))
            .expect("write");
        pw.finish().expect("finish");

        assert!(out_dir.join("slot_100.parquet").exists());
        assert!(out_dir.join("slot_101.parquet").exists());
    }
}
