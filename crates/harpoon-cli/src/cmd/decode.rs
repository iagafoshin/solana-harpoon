//! `harpoon decode` — offline decode: apply IDL to existing raw data.
//!
//! Reads raw Parquet/JSONL files, decodes both instructions and events
//! via IDL, and writes flat records partitioned by name into subdirectories
//! (same layout as `harpoon ingest --extract events/instructions`).

use {
    anyhow::Context,
    base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD},
    harpoon_decode::{Idl, IdlDecoder},
    harpoon_export::{
        FlatPartitionedWriter, FlatRecord, OutputFormat, TransactionRecord,
    },
    std::path::Path,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeMode {
    Events,
    Instructions,
    Both,
}

pub async fn run(
    idl_path: &Path,
    input_dir: &Path,
    output_dir: &Path,
    format: OutputFormat,
    mode: DecodeMode,
) -> anyhow::Result<()> {
    let idl_json =
        std::fs::read_to_string(idl_path).with_context(|| format!("read IDL: {idl_path:?}"))?;
    let idl: Idl =
        serde_json::from_str(&idl_json).with_context(|| format!("parse IDL: {idl_path:?}"))?;

    // Build schemas based on requested mode
    let mut schemas = std::collections::HashMap::new();
    if mode == DecodeMode::Events || mode == DecodeMode::Both {
        schemas.extend(crate::pipeline::build_extract_schemas(
            &idl,
            crate::pipeline::ExtractMode::Events,
        ));
    }
    if mode == DecodeMode::Instructions || mode == DecodeMode::Both {
        schemas.extend(crate::pipeline::build_extract_schemas(
            &idl,
            crate::pipeline::ExtractMode::Instructions,
        ));
    }

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

        let (ix_count, ev_count) = match ext {
            "parquet" => decode_parquet_file(input_path, &decoder, &mut pw, mode)?,
            "jsonl" => decode_jsonl_file(input_path, &decoder, &mut pw, mode)?,
            other => {
                eprintln!("  [skip] unsupported input format: {other}");
                continue;
            }
        };

        pw.finish()?;
        eprintln!("    decoded {ix_count} instructions, {ev_count} events");
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

/// Returns (instructions_decoded, events_decoded).
fn decode_records(
    records: impl Iterator<Item = TransactionRecord>,
    decoder: &IdlDecoder,
    pw: &mut FlatPartitionedWriter,
    mode: DecodeMode,
) -> anyhow::Result<(u64, u64)> {
    let do_ix = mode == DecodeMode::Instructions || mode == DecodeMode::Both;
    let do_ev = mode == DecodeMode::Events || mode == DecodeMode::Both;

    let mut buffer: Vec<FlatRecord> = Vec::new();
    let mut ix_count = 0u64;
    let mut ev_count = 0u64;

    for record in records {
        let signature = record.signatures.first().cloned().unwrap_or_default();

        // Decode instructions
        if do_ix {
            for ix in &record.instructions {
                if let Ok(raw_bytes) = bs58::decode(&ix.data).into_vec() {
                    if let Some(Ok(decoded)) = decoder.try_decode_instruction(&raw_bytes) {
                        buffer.push(make_flat_record(
                            &record, &signature, decoded.name, ix.program_id.clone(), decoded.data,
                        ));
                        ix_count += 1;
                    }
                }
            }
        }

        // Decode events from log messages
        if do_ev {
            if let Some(logs) = &record.log_messages {
                let mut program_stack: Vec<&str> = Vec::new();
                for line in logs {
                    if let Some(rest) = line.strip_prefix("Program ") {
                        if let Some(idx) = rest.find(' ') {
                            let prog_id = &rest[..idx];
                            let remainder = &rest[idx + 1..];
                            if remainder.starts_with("invoke") {
                                program_stack.push(prog_id);
                            } else if remainder.starts_with("success")
                                || remainder.starts_with("failed")
                            {
                                program_stack.pop();
                            }
                        }
                    }
                    if let Some(b64_data) = line.strip_prefix("Program data: ") {
                        if let Ok(bytes) = BASE64_STANDARD.decode(b64_data.trim()) {
                            if let Some(Ok(decoded)) = decoder.try_decode_event(&bytes) {
                                let program_id = program_stack
                                    .last()
                                    .map(|s| (*s).to_string())
                                    .unwrap_or_default();
                                buffer.push(make_flat_record(
                                    &record, &signature, decoded.name, program_id, decoded.data,
                                ));
                                ev_count += 1;
                            }
                        }
                    }
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

    Ok((ix_count, ev_count))
}

fn make_flat_record(
    record: &TransactionRecord,
    signature: &str,
    name: String,
    program_id: String,
    data: serde_json::Value,
) -> FlatRecord {
    let mut fields = match data {
        serde_json::Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    for reserved in RESERVED_NAMES {
        if let Some(val) = fields.remove(*reserved) {
            fields.insert(format!("token_{reserved}"), val);
        }
    }
    FlatRecord {
        slot: record.slot,
        block_time: record.block_time,
        signature: signature.to_string(),
        name,
        program_id,
        fields,
    }
}

fn decode_parquet_file(
    path: &Path,
    decoder: &IdlDecoder,
    pw: &mut FlatPartitionedWriter,
    mode: DecodeMode,
) -> anyhow::Result<(u64, u64)> {
    let iter = harpoon_export::read_parquet_records(path)
        .with_context(|| format!("open parquet: {path:?}"))?;

    let records = iter.filter_map(|r| match r {
        Ok(rec) => Some(rec),
        Err(e) => {
            eprintln!("    [warn] skipping record: {e}");
            None
        }
    });

    decode_records(records, decoder, pw, mode)
}

fn decode_jsonl_file(
    path: &Path,
    decoder: &IdlDecoder,
    pw: &mut FlatPartitionedWriter,
    mode: DecodeMode,
) -> anyhow::Result<(u64, u64)> {
    use std::io::{BufRead, BufReader};

    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);

    let records = reader.lines().filter_map(|line| {
        let line = line.ok()?;
        serde_json::from_str::<TransactionRecord>(&line).ok()
    });

    decode_records(records, decoder, pw, mode)
}
