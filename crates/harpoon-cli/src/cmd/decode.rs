//! `harpoon decode` — offline decode: apply IDL to existing raw data.
//!
//! Reads raw Parquet/JSONL files, decodes instructions via IDL, and writes
//! flat records partitioned by instruction name into subdirectories
//! (same layout as `harpoon ingest --extract instructions`).

use {
    anyhow::Context,
    harpoon_decode::{Idl, IdlDecoder},
    harpoon_export::{
        FlatPartitionedWriter, FlatRecord, OutputFormat, TransactionRecord,
    },
    std::path::Path,
};

pub async fn run(
    idl_path: &Path,
    input_dir: &Path,
    output_dir: &Path,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let idl_json =
        std::fs::read_to_string(idl_path).with_context(|| format!("read IDL: {idl_path:?}"))?;
    let idl: Idl =
        serde_json::from_str(&idl_json).with_context(|| format!("parse IDL: {idl_path:?}"))?;
    let schemas = crate::pipeline::build_extract_schemas(&idl, crate::pipeline::ExtractMode::Instructions);
    let decoder = IdlDecoder::from_idl(idl);

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

    for input_path in &inputs {
        eprintln!("  Processing {input_path:?}");
        let epoch = extract_epoch_from_filename(input_path);
        let mut pw = FlatPartitionedWriter::new(output_dir, format, schemas.clone(), epoch)?;

        let ext = input_path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");

        let decoded_count = match ext {
            "parquet" => decode_parquet_file(input_path, &decoder, &mut pw)?,
            "jsonl" => decode_jsonl_file(input_path, &decoder, &mut pw)?,
            other => {
                eprintln!("  [skip] unsupported input format: {other}");
                continue;
            }
        };

        pw.finish()?;
        eprintln!("    decoded {decoded_count} instructions");
    }

    eprintln!("Decode complete → {output_dir:?}");
    Ok(())
}

/// Extract epoch number from filenames like `epoch-945.parquet`.
/// Falls back to 0 if the pattern doesn't match.
fn extract_epoch_from_filename(path: &Path) -> u64 {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| {
            s.strip_prefix("epoch-")
                .or_else(|| s.strip_prefix("epoch_"))
                .and_then(|rest| rest.parse::<u64>().ok())
        })
        .unwrap_or(0)
}

const RESERVED_NAMES: &[&str] = &["slot", "block_time", "signature", "name", "program_id"];

fn decode_records(
    records: impl Iterator<Item = TransactionRecord>,
    decoder: &IdlDecoder,
    pw: &mut FlatPartitionedWriter,
) -> anyhow::Result<u64> {
    let mut buffer: Vec<FlatRecord> = Vec::new();
    let mut decoded_count = 0u64;

    for record in records {
        let signature = record.signatures.first().cloned().unwrap_or_default();

        for ix in &record.instructions {
            if let Ok(raw_bytes) = bs58::decode(&ix.data).into_vec() {
                if let Some(Ok(decoded)) = decoder.try_decode_instruction(&raw_bytes) {
                    let mut fields = match decoded.data {
                        serde_json::Value::Object(map) => map,
                        _ => serde_json::Map::new(),
                    };
                    // Rename fields that collide with FlatRecord common fields
                    for reserved in RESERVED_NAMES {
                        if let Some(val) = fields.remove(*reserved) {
                            fields.insert(format!("token_{reserved}"), val);
                        }
                    }
                    buffer.push(FlatRecord {
                        slot: record.slot,
                        block_time: record.block_time,
                        signature: signature.clone(),
                        name: decoded.name,
                        program_id: ix.program_id.clone(),
                        fields,
                    });
                    decoded_count += 1;
                }
            }
        }

        if buffer.len() >= 10_000 {
            pw.write_records(&buffer)?;
            buffer.clear();
        }
    }

    if !buffer.is_empty() {
        pw.write_records(&buffer)?;
    }

    Ok(decoded_count)
}

fn decode_parquet_file(
    path: &Path,
    decoder: &IdlDecoder,
    pw: &mut FlatPartitionedWriter,
) -> anyhow::Result<u64> {
    let iter = harpoon_export::read_parquet_records(path)
        .with_context(|| format!("open parquet: {path:?}"))?;

    let records = iter.filter_map(|r| match r {
        Ok(rec) => Some(rec),
        Err(e) => {
            eprintln!("    [warn] skipping record: {e}");
            None
        }
    });

    decode_records(records, decoder, pw)
}

fn decode_jsonl_file(
    path: &Path,
    decoder: &IdlDecoder,
    pw: &mut FlatPartitionedWriter,
) -> anyhow::Result<u64> {
    use std::io::{BufRead, BufReader};

    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);

    let records = reader.lines().filter_map(|line| {
        let line = line.ok()?;
        serde_json::from_str::<TransactionRecord>(&line).ok()
    });

    decode_records(records, decoder, pw)
}
