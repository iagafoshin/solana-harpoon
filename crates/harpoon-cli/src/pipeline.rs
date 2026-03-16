//! Three-stage ingest pipeline: reader → rayon → writer.
//!
//! ```text
//! Stage 1 (I/O):        Stage 2 (CPU):              Stage 3 (I/O):
//! tokio / mmap           rayon thread pool            dedicated thread
//! reads CAR nodes   →    decode tx + filter      →    builds Arrow batches
//! groups by block        decode IDL (if enabled)      writes output
//!
//!     ╔═══════════╗      ╔═══════════╗              ╔═══════════╗
//!     ║  bounded   ║      ║  bounded   ║              ║  bounded   ║
//!     ║  channel   ║─────▶║  channel   ║─────────────▶║  channel   ║
//!     ║  (tx data) ║      ║  (records) ║              ║  (batches) ║
//!     ╚═══════════╝      ╚═══════════╝              ╚═══════════╝
//! ```

use {
    crate::stats::{PipelineStats, TimingStats},
    anyhow::{anyhow, Context},
    bytes::Bytes,
    harpoon_car::node::{Node, Nodes},
    harpoon_decode::IdlDecoder,
    harpoon_export::{
        ExportWriter, TransactionRecord,
        record::{AccountBalanceDelta, InstructionRecord, TokenBalanceDelta},
    },
    indicatif::{ProgressBar, ProgressStyle},
    rayon::prelude::*,
    solana_sdk::{message::VersionedMessage, pubkey::Pubkey},
    std::{
        collections::HashSet,
        convert::TryFrom,
        sync::{
            atomic::Ordering,
            mpsc::{sync_channel, SyncSender},
            Arc,
        },
        time::Instant,
    },
    tokio::io::AsyncRead,
};

/// A prepared transaction ready for CPU processing.
struct PreparedTx {
    slot: u64,
    block_time: Option<i64>,
    tx_data: Bytes,
    meta_data: Option<Bytes>,
}

const RAYON_CHUNK_SIZE: usize = 5_000;
const CHANNEL_BOUND: usize = 4;

/// Run the 3-stage pipeline on an async reader.
pub async fn run_async_pipeline(
    input: Box<dyn AsyncRead + Unpin + Send>,
    target_programs: Arc<HashSet<Pubkey>>,
    idl_decoder: Option<Arc<IdlDecoder>>,
    mut writer: Box<dyn ExportWriter + Send>,
    batch_size: usize,
    stats: Arc<PipelineStats>,
    timing: Arc<TimingStats>,
) -> anyhow::Result<()> {
    let mut reader = harpoon_car::node::NodeReader::new(tokio::io::BufReader::new(input));

    // Channel: CPU stage → writer thread
    let (writer_tx, writer_rx) = sync_channel::<Vec<TransactionRecord>>(CHANNEL_BOUND);

    // Stage 3: dedicated writer thread
    let writer_timing = Arc::clone(&timing);
    let writer_handle = std::thread::spawn(move || -> anyhow::Result<()> {
        for batch in writer_rx.iter() {
            let start = Instant::now();
            writer.write_records(&batch)?;
            writer_timing.record(
                &writer_timing.parquet_write_ns,
                &writer_timing.parquet_write_samples,
                start.elapsed(),
                true,
            );
        }
        writer.finish()?;
        Ok(())
    });

    // Progress bar
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] {msg}",
        )
        .expect("valid template"),
    );

    let start_time = Instant::now();
    let mut buffer: Vec<TransactionRecord> = Vec::new();

    // Stage 1: async read CAR nodes
    loop {
        let nodes = Nodes::read_until_block(&mut reader).await?;
        if nodes.nodes.is_empty() {
            break;
        }

        let block_time = nodes.nodes.values().find_map(|node| match node {
            Node::Block(block) => i64::try_from(block.meta.blocktime).ok(),
            _ => None,
        });

        let mut prepared = Vec::new();

        for node in nodes.nodes.values() {
            match node {
                Node::Transaction(frame) => {
                    let total =
                        stats.total_transactions.fetch_add(1, Ordering::Relaxed) + 1;
                    if total % 10_000 == 0 {
                        let matched = stats.matching_transactions.load(Ordering::Relaxed);
                        let speed = total as f64 / start_time.elapsed().as_secs_f64();
                        pb.set_message(format!(
                            "tx={total} matched={matched} speed={:.0} tx/s slot={}",
                            speed, frame.slot
                        ));
                    }

                    let meta_data = match nodes.reassemble_dataframes(&frame.metadata) {
                        Ok(bytes) if bytes.is_empty() => None,
                        Ok(bytes) => Some(Bytes::from(bytes)),
                        Err(err) => {
                            eprintln!(
                                "[warn] slot={} failed to reassemble metadata: {err}",
                                frame.slot
                            );
                            stats.metadata_errors.fetch_add(1, Ordering::Relaxed);
                            None
                        }
                    };

                    prepared.push(PreparedTx {
                        slot: frame.slot,
                        block_time,
                        tx_data: Bytes::from(frame.data.data.clone()),
                        meta_data,
                    });
                }
                Node::Block(_) => {
                    stats.total_blocks.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
        }

        // Stage 2: process in rayon chunks
        process_chunks(
            &mut prepared,
            &target_programs,
            &idl_decoder,
            &stats,
            &timing,
            &mut buffer,
            &writer_tx,
            batch_size,
        )?;
    }

    // Flush remaining
    if !buffer.is_empty() {
        writer_tx
            .send(std::mem::take(&mut buffer))
            .map_err(|_| anyhow!("writer thread gone"))?;
    }
    drop(writer_tx);

    writer_handle
        .join()
        .map_err(|_| anyhow!("writer thread panicked"))??;

    pb.finish_and_clear();
    print_summary(&stats, start_time);
    timing.print_summary();

    Ok(())
}

/// Run the 3-stage pipeline on a memory-mapped CAR file.
pub fn run_mmap_pipeline(
    car_path: &std::path::Path,
    target_programs: Arc<HashSet<Pubkey>>,
    idl_decoder: Option<Arc<IdlDecoder>>,
    mut writer: Box<dyn ExportWriter + Send>,
    batch_size: usize,
    stats: Arc<PipelineStats>,
    timing: Arc<TimingStats>,
) -> anyhow::Result<()> {
    let mmap_reader = harpoon_car::MmapNodeReader::open(car_path)
        .with_context(|| format!("failed to mmap {car_path:?}"))?;

    // Channel: CPU stage → writer thread
    let (writer_tx, writer_rx) = sync_channel::<Vec<TransactionRecord>>(CHANNEL_BOUND);

    let writer_timing = Arc::clone(&timing);
    let writer_handle = std::thread::spawn(move || -> anyhow::Result<()> {
        for batch in writer_rx.iter() {
            let start = Instant::now();
            writer.write_records(&batch)?;
            writer_timing.record(
                &writer_timing.parquet_write_ns,
                &writer_timing.parquet_write_samples,
                start.elapsed(),
                true,
            );
        }
        writer.finish()?;
        Ok(())
    });

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {msg}")
            .expect("valid template"),
    );

    let start_time = Instant::now();
    let mut buffer: Vec<TransactionRecord> = Vec::new();
    let mut prepared: Vec<PreparedTx> = Vec::new();

    for raw_result in mmap_reader {
        let raw = match raw_result {
            Ok(raw) => raw,
            Err(err) => {
                eprintln!("[warn] node read error: {err}");
                continue;
            }
        };

        let node = match Node::try_from(raw.get_data()) {
            Ok(node) => node,
            Err(err) => {
                eprintln!("[warn] node parse error: {err}");
                continue;
            }
        };

        match node {
            Node::Transaction(frame) => {
                let total = stats.total_transactions.fetch_add(1, Ordering::Relaxed) + 1;
                if total % 10_000 == 0 {
                    let matched = stats.matching_transactions.load(Ordering::Relaxed);
                    let speed = total as f64 / start_time.elapsed().as_secs_f64();
                    pb.set_message(format!(
                        "tx={total} matched={matched} speed={speed:.0} tx/s slot={}",
                        frame.slot
                    ));
                }

                // For mmap path, data is inline (single DataFrame, no reassembly needed for tx data).
                // Metadata may need reassembly in a full Nodes context, but for mmap we read it directly.
                // Since we don't have Nodes context, we use the inline data.
                prepared.push(PreparedTx {
                    slot: frame.slot,
                    block_time: None, // mmap path doesn't group by block
                    tx_data: Bytes::from(frame.data.data.clone()),
                    meta_data: if frame.metadata.data.is_empty() {
                        None
                    } else {
                        Some(Bytes::from(frame.metadata.data.clone()))
                    },
                });

                if prepared.len() >= RAYON_CHUNK_SIZE {
                    process_chunks(
                        &mut prepared,
                        &target_programs,
                        &idl_decoder,
                        &stats,
                        &timing,
                        &mut buffer,
                        &writer_tx,
                        batch_size,
                    )?;
                }
            }
            Node::Block(_) => {
                stats.total_blocks.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    // Drain remaining prepared txs
    if !prepared.is_empty() {
        process_chunks(
            &mut prepared,
            &target_programs,
            &idl_decoder,
            &stats,
            &timing,
            &mut buffer,
            &writer_tx,
            batch_size,
        )?;
    }

    if !buffer.is_empty() {
        writer_tx
            .send(std::mem::take(&mut buffer))
            .map_err(|_| anyhow!("writer thread gone"))?;
    }
    drop(writer_tx);

    writer_handle
        .join()
        .map_err(|_| anyhow!("writer thread panicked"))??;

    pb.finish_and_clear();
    print_summary(&stats, start_time);
    timing.print_summary();

    Ok(())
}

/// Process prepared transactions through rayon, drain into buffer, send full batches.
#[allow(clippy::too_many_arguments)]
fn process_chunks(
    prepared: &mut Vec<PreparedTx>,
    target_programs: &HashSet<Pubkey>,
    idl_decoder: &Option<Arc<IdlDecoder>>,
    stats: &PipelineStats,
    timing: &TimingStats,
    buffer: &mut Vec<TransactionRecord>,
    writer_tx: &SyncSender<Vec<TransactionRecord>>,
    batch_size: usize,
) -> anyhow::Result<()> {
    while prepared.len() >= RAYON_CHUNK_SIZE {
        let chunk: Vec<PreparedTx> = prepared.drain(..RAYON_CHUNK_SIZE).collect();
        let records = process_batch(chunk, target_programs, idl_decoder, stats, timing);
        buffer.extend(records);
        flush_if_full(buffer, writer_tx, batch_size)?;
    }

    // Process remaining
    if !prepared.is_empty() {
        let chunk: Vec<PreparedTx> = std::mem::take(prepared);
        let records = process_batch(chunk, target_programs, idl_decoder, stats, timing);
        buffer.extend(records);
        flush_if_full(buffer, writer_tx, batch_size)?;
    }

    Ok(())
}

fn flush_if_full(
    buffer: &mut Vec<TransactionRecord>,
    writer_tx: &SyncSender<Vec<TransactionRecord>>,
    batch_size: usize,
) -> anyhow::Result<()> {
    while buffer.len() >= batch_size {
        let batch: Vec<TransactionRecord> = buffer.drain(..batch_size).collect();
        writer_tx
            .send(batch)
            .map_err(|_| anyhow!("writer thread gone"))?;
    }
    Ok(())
}

/// Parallel process a chunk of prepared transactions via rayon.
fn process_batch(
    prepared: Vec<PreparedTx>,
    target_programs: &HashSet<Pubkey>,
    idl_decoder: &Option<Arc<IdlDecoder>>,
    stats: &PipelineStats,
    timing: &TimingStats,
) -> Vec<TransactionRecord> {
    prepared
        .into_par_iter()
        .filter_map(|ptx| {
            match process_single_tx(ptx, target_programs, idl_decoder, timing) {
                Ok(Some(record)) => {
                    stats.matching_transactions.fetch_add(1, Ordering::Relaxed);
                    Some(record)
                }
                Ok(None) => None,
                Err(_) => {
                    stats.decode_errors.fetch_add(1, Ordering::Relaxed);
                    None
                }
            }
        })
        .collect()
}

/// Process a single prepared transaction: decode → filter → build record.
fn process_single_tx(
    ptx: PreparedTx,
    target_programs: &HashSet<Pubkey>,
    idl_decoder: &Option<Arc<IdlDecoder>>,
    timing: &TimingStats,
) -> anyhow::Result<Option<TransactionRecord>> {
    let sample = timing.should_sample();

    // Decode transaction
    let t0 = Instant::now();
    let tx = harpoon_solana::decode_transaction(&ptx.tx_data)
        .context("tx decode")?;
    timing.record(&timing.tx_decode_ns, &timing.tx_decode_samples, t0.elapsed(), sample);

    // Decode metadata
    let t1 = Instant::now();
    let meta = ptx
        .meta_data
        .as_deref()
        .map(harpoon_solana::decode_metadata)
        .transpose()
        .ok()
        .flatten()
        .flatten();
    timing.record(&timing.meta_decode_ns, &timing.meta_decode_samples, t1.elapsed(), sample);

    // Resolve keys
    let account_keys = harpoon_solana::resolve_full_account_keys(&tx, meta.as_ref());

    // Get outer instructions for filter
    let outer_instructions = match &tx.message {
        VersionedMessage::Legacy(msg) => &msg.instructions,
        VersionedMessage::V0(msg) => &msg.instructions,
    };

    // Filter
    let t2 = Instant::now();
    let target_slice: Vec<Pubkey> = target_programs.iter().copied().collect();
    let matched =
        harpoon_solana::matches_programs(&target_slice, &account_keys, outer_instructions, meta.as_ref());
    timing.record(&timing.match_check_ns, &timing.match_check_samples, t2.elapsed(), sample);

    if !matched {
        return Ok(None);
    }

    // Build record
    let t3 = Instant::now();
    let program_ids = harpoon_solana::collect_program_ids(&account_keys, &tx, meta.as_ref());
    let instructions = harpoon_solana::extract_instructions(&account_keys, &tx, meta.as_ref());

    let account_balance_deltas = meta
        .as_ref()
        .map(|m| {
            harpoon_solana::build_account_balance_deltas(m, &account_keys)
                .into_iter()
                .map(|d| AccountBalanceDelta {
                    account: d.account,
                    pre_lamports: d.pre_lamports,
                    post_lamports: d.post_lamports,
                    delta_lamports: d.delta_lamports,
                })
                .collect()
        })
        .unwrap_or_default();

    let token_balance_deltas = meta
        .as_ref()
        .map(|m| {
            harpoon_solana::build_token_balance_deltas(m)
                .into_iter()
                .map(|d| TokenBalanceDelta {
                    mint: d.mint,
                    owner: d.owner,
                    account_index: d.account_index,
                    pre_ui_amount: d.pre_ui_amount,
                    post_ui_amount: d.post_ui_amount,
                })
                .collect()
        })
        .unwrap_or_default();

    let fee = meta.as_ref().map(|m| m.fee).unwrap_or_default();

    // Build instruction records, optionally decoding via IDL
    let instruction_records: Vec<InstructionRecord> = instructions
        .iter()
        .map(|ix| {
            let mut rec = InstructionRecord {
                program_id: ix.program_id.clone(),
                data: ix.data.clone(),
                accounts: ix.accounts.clone(),
            };

            // If IDL decoder is present, try to decode and replace data with JSON
            if let Some(decoder) = idl_decoder {
                if let Ok(raw_bytes) = bs58::decode(&ix.data).into_vec() {
                    if let Some(Ok(decoded)) = decoder.try_decode_instruction(&raw_bytes) {
                        if let Ok(json_str) = serde_json::to_string(&serde_json::json!({
                            "name": decoded.name,
                            "args": decoded.data,
                        })) {
                            rec.data = json_str;
                        }
                    }
                }
            }
            rec
        })
        .collect();

    let record = TransactionRecord {
        slot: ptx.slot,
        block_time: ptx.block_time,
        signatures: tx.signatures.iter().map(|s| s.to_string()).collect(),
        accounts: account_keys.iter().map(|k| k.to_string()).collect(),
        program_ids: program_ids.iter().map(|k| k.to_string()).collect(),
        fee,
        account_balance_deltas,
        token_balance_deltas,
        log_messages: meta
            .as_ref()
            .and_then(|m| m.log_messages.clone()),
        err: meta
            .as_ref()
            .and_then(|m| m.status.as_ref().err().map(|e| format!("{e:?}"))),
        instructions: instruction_records,
    };

    timing.record(&timing.build_record_ns, &timing.build_record_samples, t3.elapsed(), sample);
    Ok(Some(record))
}

fn print_summary(stats: &PipelineStats, start_time: Instant) {
    let elapsed = start_time.elapsed();
    let total = stats.total_transactions.load(Ordering::Relaxed);
    let matched = stats.matching_transactions.load(Ordering::Relaxed);
    let metadata_errors = stats.metadata_errors.load(Ordering::Relaxed);
    let decode_errors = stats.decode_errors.load(Ordering::Relaxed);
    let blocks = stats.total_blocks.load(Ordering::Relaxed);
    let speed = if elapsed.as_secs_f64() > 0.0 {
        (total as f64 / elapsed.as_secs_f64()) as u64
    } else {
        0
    };

    eprintln!("[done]");
    eprintln!(
        "  total tx:            {}",
        crate::stats::format_number(total)
    );
    eprintln!(
        "  matched tx:          {}",
        crate::stats::format_number(matched)
    );
    eprintln!(
        "  metadata errors:     {}",
        crate::stats::format_number(metadata_errors)
    );
    eprintln!(
        "  decode errors:       {}",
        crate::stats::format_number(decode_errors)
    );
    eprintln!(
        "  blocks:              {}",
        crate::stats::format_number(blocks)
    );
    eprintln!(
        "  elapsed:             {}",
        crate::stats::format_duration(elapsed)
    );
    eprintln!(
        "  avg speed:           {} tx/s",
        crate::stats::format_number(speed)
    );
}
