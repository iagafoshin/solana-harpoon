//! `harpoon decode` — offline decode: apply IDL to existing raw data.

use {
    anyhow::Context,
    harpoon_decode::IdlDecoder,
    harpoon_export::{
        OutputFormat, PartitionedWriter, TransactionRecord,
        record::InstructionRecord,
    },
    std::path::Path,
};

pub async fn run(
    idl_path: &Path,
    input_dir: &Path,
    output_dir: &Path,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let decoder =
        IdlDecoder::from_idl_path(idl_path).with_context(|| format!("load IDL: {idl_path:?}"))?;

    // Find input files
    let mut inputs: Vec<_> = std::fs::read_dir(input_dir)
        .with_context(|| format!("read dir: {input_dir:?}"))?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .is_some_and(|ext| ext == "jsonl" || ext == "parquet" || ext == "csv")
        })
        .map(|e| e.path())
        .collect();
    inputs.sort();

    if inputs.is_empty() {
        eprintln!("No input files found in {input_dir:?}");
        return Ok(());
    }

    eprintln!(
        "Decoding {} file(s) with IDL from {idl_path:?}",
        inputs.len()
    );

    let mut pw = PartitionedWriter::new(output_dir, format, 50_000)?;

    for input_path in &inputs {
        eprintln!("  Processing {input_path:?}");
        let ext = input_path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");

        match ext {
            "parquet" => decode_parquet_file(input_path, &decoder, &mut pw)?,
            "jsonl" => decode_jsonl_file(input_path, &decoder, &mut pw)?,
            other => {
                eprintln!("  [skip] unsupported input format: {other}");
            }
        }
    }

    pw.finish()?;
    eprintln!("Decode complete → {output_dir:?}");
    Ok(())
}

fn decode_records(
    records: impl Iterator<Item = TransactionRecord>,
    decoder: &IdlDecoder,
    pw: &mut PartitionedWriter,
) -> anyhow::Result<u64> {
    let mut batch: Vec<TransactionRecord> = Vec::new();
    let mut decoded_count = 0u64;

    for record in records {
        let mut decoded_instructions = Vec::new();
        for ix in &record.instructions {
            if let Ok(raw_bytes) = bs58::decode(&ix.data).into_vec() {
                if let Some(Ok(decoded)) = decoder.try_decode_instruction(&raw_bytes) {
                    decoded_instructions.push(InstructionRecord {
                        program_id: ix.program_id.clone(),
                        data: serde_json::to_string(&serde_json::json!({
                            "name": decoded.name,
                            "args": decoded.data,
                        }))?,
                        accounts: ix.accounts.clone(),
                    });
                    decoded_count += 1;
                    continue;
                }
            }
            // Keep original if decode fails
            decoded_instructions.push(ix.clone());
        }

        let mut decoded_record = record;
        decoded_record.instructions = decoded_instructions;
        batch.push(decoded_record);

        if batch.len() >= 10_000 {
            pw.write_partitioned(&batch, partition_key)?;
            batch.clear();
        }
    }

    if !batch.is_empty() {
        pw.write_partitioned(&batch, partition_key)?;
    }

    Ok(decoded_count)
}

fn decode_parquet_file(
    path: &Path,
    decoder: &IdlDecoder,
    pw: &mut PartitionedWriter,
) -> anyhow::Result<()> {
    let iter = harpoon_export::read_parquet_records(path)
        .with_context(|| format!("open parquet: {path:?}"))?;

    // Filter out read errors, logging them
    let records = iter.filter_map(|r| match r {
        Ok(rec) => Some(rec),
        Err(e) => {
            eprintln!("    [warn] skipping record: {e}");
            None
        }
    });

    let decoded_count = decode_records(records, decoder, pw)?;
    eprintln!("    decoded {decoded_count} instructions");
    Ok(())
}

fn decode_jsonl_file(
    path: &Path,
    decoder: &IdlDecoder,
    pw: &mut PartitionedWriter,
) -> anyhow::Result<()> {
    use std::io::{BufRead, BufReader};

    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);

    let records = reader.lines().filter_map(|line| {
        let line = line.ok()?;
        serde_json::from_str::<TransactionRecord>(&line).ok()
    });

    let decoded_count = decode_records(records, decoder, pw)?;
    eprintln!("    decoded {decoded_count} instructions");
    Ok(())
}

/// Partition key: first decoded instruction name, or "unknown".
fn partition_key(r: &TransactionRecord) -> String {
    for ix in &r.instructions {
        if ix.data.starts_with('{') {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&ix.data) {
                if let Some(name) = v.get("name").and_then(|n| n.as_str()) {
                    return name.to_string();
                }
            }
        }
    }
    "unknown".to_string()
}
