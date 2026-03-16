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
    base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD},
    bytes::Bytes,
    harpoon_car::node::{Kind, Node, Nodes},
    harpoon_decode::{
        Idl, IdlDecoder,
        idl::{IdlType, IdlTypeComplex, IdlTypeDefBody, StructFields},
    },
    harpoon_export::{
        ArrowDataType, ArrowField, ArrowSchemaRef,
        ExportWriter, FlatPartitionedWriter, FlatRecord, TransactionRecord,
        build_flat_schema,
        record::{AccountBalanceDelta, InstructionRecord, TokenBalanceDelta},
    },
    indicatif::{ProgressBar, ProgressStyle},
    rayon::prelude::*,
    solana_sdk::{message::VersionedMessage, pubkey::Pubkey},
    std::{
        cell::RefCell,
        collections::{HashMap, HashSet},
        sync::{
            atomic::Ordering,
            mpsc::{SyncSender, sync_channel},
            Arc,
        },
        time::Instant,
    },
    tokio::io::AsyncRead,
};

/// Node kinds needed by the pipeline. Entry, Subset, Epoch, Rewards are
/// skipped — their CBOR is never deserialized, saving ~60-70% of parse work.
const PIPELINE_KEEP_KINDS: &[Kind] = &[Kind::Transaction, Kind::Block, Kind::DataFrame];

/// Extraction mode for the ingest pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractMode {
    /// Full transaction records (default).
    Raw,
    /// Decoded events from log messages, partitioned by event name.
    Events,
    /// Decoded instructions, partitioned by instruction name.
    Instructions,
}

/// A prepared transaction ready for CPU processing.
struct PreparedTx {
    slot: u64,
    block_time: Option<i64>,
    tx_data: Bytes,
    meta_data: Option<Bytes>,
}

const RAYON_CHUNK_SIZE: usize = 5_000;
const CHANNEL_BOUND: usize = 4;

thread_local! {
    static ZSTD_STATE: RefCell<(zstd::bulk::Decompressor<'static>, Vec<u8>)> = RefCell::new((
        zstd::bulk::Decompressor::new().expect("zstd init"),
        Vec::with_capacity(4096),
    ));
}

// ===========================================================================
// Schema building from IDL
// ===========================================================================

/// Map an IDL type to an Arrow DataType for flat Parquet output.
fn idl_type_to_arrow(
    ty: &IdlType,
    types: &HashMap<String, &harpoon_decode::idl::IdlTypeDef>,
) -> ArrowDataType {
    match ty {
        IdlType::Primitive(s) => match s.as_str() {
            "u8" => ArrowDataType::UInt8,
            "u16" => ArrowDataType::UInt16,
            "u32" => ArrowDataType::UInt32,
            "u64" => ArrowDataType::UInt64,
            "i8" => ArrowDataType::Int8,
            "i16" => ArrowDataType::Int16,
            "i32" => ArrowDataType::Int32,
            "i64" => ArrowDataType::Int64,
            "f32" => ArrowDataType::Float32,
            "f64" => ArrowDataType::Float64,
            "bool" => ArrowDataType::Boolean,
            // pubkey, string, u128, i128, bytes → Utf8
            _ => ArrowDataType::Utf8,
        },
        IdlType::Complex(IdlTypeComplex::Option(inner)) => idl_type_to_arrow(inner, types),
        IdlType::Complex(IdlTypeComplex::Defined(r)) => {
            // Unwrap simple newtypes (e.g. OptionBool wrapping bool)
            if let Some(td) = types.get(&r.name) {
                if let IdlTypeDefBody::Struct {
                    fields: StructFields::Tuple(ref tys),
                } = td.ty
                {
                    if tys.len() == 1 {
                        return idl_type_to_arrow(&tys[0], types);
                    }
                }
            }
            ArrowDataType::Utf8
        }
        _ => ArrowDataType::Utf8,
    }
}

/// Build per-name Arrow schemas from an IDL for extract modes.
pub fn build_extract_schemas(idl: &Idl, mode: ExtractMode) -> HashMap<String, ArrowSchemaRef> {
    let type_map = idl.type_map();
    let mut schemas = HashMap::new();

    match mode {
        ExtractMode::Events => {
            for event in &idl.events {
                if let Some(td) = type_map.get(&event.name) {
                    if let IdlTypeDefBody::Struct {
                        fields: StructFields::Named(ref named),
                    } = td.ty
                    {
                        let extra: Vec<ArrowField> = named
                            .iter()
                            .map(|f| {
                                let dt = idl_type_to_arrow(&f.ty, &type_map);
                                let nullable =
                                    matches!(&f.ty, IdlType::Complex(IdlTypeComplex::Option(_)));
                                ArrowField::new(&f.name, dt, nullable)
                            })
                            .collect();
                        schemas.insert(event.name.clone(), build_flat_schema(&extra));
                    }
                }
            }
        }
        ExtractMode::Instructions => {
            for ix in &idl.instructions {
                let extra: Vec<ArrowField> = ix
                    .args
                    .iter()
                    .map(|f| {
                        let dt = idl_type_to_arrow(&f.ty, &type_map);
                        let nullable =
                            matches!(&f.ty, IdlType::Complex(IdlTypeComplex::Option(_)));
                        ArrowField::new(&f.name, dt, nullable)
                    })
                    .collect();
                schemas.insert(ix.name.clone(), build_flat_schema(&extra));
            }
        }
        ExtractMode::Raw => {}
    }

    schemas
}

// ===========================================================================
// Raw pipeline
// ===========================================================================

/// Run the 3-stage pipeline on an async reader (raw mode).
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

    loop {
        let nodes = Nodes::read_until_block_filtered(&mut reader, Some(PIPELINE_KEEP_KINDS)).await?;
        if nodes.nodes.is_empty() {
            break;
        }

        let (_block_time, mut prepared) = prepare_nodes(nodes, &stats, start_time, &pb);

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

// ===========================================================================
// Extract pipeline (events / instructions)
// ===========================================================================

/// Run the extract pipeline: decode → filter → extract events/instructions → flat output.
#[allow(clippy::too_many_arguments)]
pub async fn run_extract_pipeline(
    input: Box<dyn AsyncRead + Unpin + Send>,
    target_programs: Arc<HashSet<Pubkey>>,
    idl_decoder: Arc<IdlDecoder>,
    extract_mode: ExtractMode,
    writer: FlatPartitionedWriter,
    batch_size: usize,
    stats: Arc<PipelineStats>,
    timing: Arc<TimingStats>,
) -> anyhow::Result<()> {
    let mut reader = harpoon_car::node::NodeReader::new(tokio::io::BufReader::new(input));

    let (writer_tx, writer_rx) = sync_channel::<Vec<FlatRecord>>(CHANNEL_BOUND);

    let writer_timing = Arc::clone(&timing);
    let writer_handle = std::thread::spawn(move || -> anyhow::Result<()> {
        let mut writer = writer;
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
    let mut buffer: Vec<FlatRecord> = Vec::new();

    loop {
        let nodes = Nodes::read_until_block_filtered(&mut reader, Some(PIPELINE_KEEP_KINDS)).await?;
        if nodes.nodes.is_empty() {
            break;
        }

        let (_block_time, mut prepared) = prepare_nodes(nodes, &stats, start_time, &pb);

        process_extract_chunks(
            &mut prepared,
            &target_programs,
            &idl_decoder,
            extract_mode,
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

// ===========================================================================
// Shared helpers
// ===========================================================================

/// Prepare CAR nodes into `PreparedTx` entries.
///
/// Takes `Nodes` by value so transaction data can be moved instead of cloned.
/// Metadata is reassembled in a first pass (needs shared references to
/// look up DataFrame CIDs), then the map is drained to move tx bytes out.
fn prepare_nodes(
    nodes: Nodes,
    stats: &PipelineStats,
    start_time: Instant,
    pb: &ProgressBar,
) -> (Option<i64>, Vec<PreparedTx>) {
    let block_time = nodes.nodes.values().find_map(|node| match node {
        Node::Block(block) => i64::try_from(block.meta.blocktime).ok(),
        _ => None,
    });

    // Phase 1: reassemble metadata while we still have shared access to
    // the full node map (needed for multi-frame DataFrame CID lookups).
    let mut meta_by_idx: Vec<(usize, u64, Option<Bytes>)> = Vec::new();
    for (idx, node) in nodes.nodes.values().enumerate() {
        if let Node::Transaction(frame) = node {
            let total = stats.total_transactions.fetch_add(1, Ordering::Relaxed) + 1;
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

            meta_by_idx.push((idx, frame.slot, meta_data));
        }
    }

    // Phase 2: drain the map to take ownership, moving tx data instead of
    // cloning. We match by index so metadata lines up with the right tx.
    let mut meta_iter = meta_by_idx.into_iter().peekable();
    let mut prepared = Vec::with_capacity(meta_iter.len());

    for (idx, (_cid, node)) in nodes.nodes.into_iter().enumerate() {
        match node {
            Node::Transaction(frame) => {
                let (_mi, _slot, meta_data) = meta_iter
                    .next()
                    .expect("metadata count must match transaction count");
                debug_assert_eq!(_mi, idx);

                prepared.push(PreparedTx {
                    slot: frame.slot,
                    block_time,
                    tx_data: Bytes::from(frame.data.data), // moved, not cloned
                    meta_data,
                });
            }
            Node::Block(_) => {
                stats.total_blocks.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    (block_time, prepared)
}

// ===========================================================================
// Raw pipeline processing
// ===========================================================================

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
            match process_single_tx(ptx, target_programs, idl_decoder, stats, timing) {
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

fn process_single_tx(
    ptx: PreparedTx,
    target_programs: &HashSet<Pubkey>,
    idl_decoder: &Option<Arc<IdlDecoder>>,
    stats: &PipelineStats,
    timing: &TimingStats,
) -> anyhow::Result<Option<TransactionRecord>> {
    let sample = timing.should_sample();

    // Phase 1: decode transaction (cheap ~2μs bincode) and check outer instructions.
    let t0 = Instant::now();
    let tx =
        harpoon_solana::decode_transaction(&ptx.tx_data).context("tx decode")?;
    timing.record(
        &timing.tx_decode_ns,
        &timing.tx_decode_samples,
        t0.elapsed(),
        sample,
    );

    let static_keys = match &tx.message {
        VersionedMessage::Legacy(msg) => &msg.account_keys,
        VersionedMessage::V0(msg) => &msg.account_keys,
    };
    let outer_instructions = match &tx.message {
        VersionedMessage::Legacy(msg) => &msg.instructions,
        VersionedMessage::V0(msg) => &msg.instructions,
    };

    let t2 = Instant::now();
    let target_slice: Vec<Pubkey> = target_programs.iter().copied().collect();
    let outer_matched =
        harpoon_solana::matches_outer_programs(&target_slice, static_keys, outer_instructions);
    timing.record(
        &timing.match_check_ns,
        &timing.match_check_samples,
        t2.elapsed(),
        sample,
    );

    // Fast reject: if the target program isn't even in the static account keys,
    // it can't appear in inner instructions either (for legacy txs this is exact;
    // for v0 it's conservative since loaded ALT addresses are rare program targets).
    if !outer_matched && !harpoon_solana::could_match_programs(&target_slice, static_keys) {
        stats.metadata_skipped.fetch_add(1, Ordering::Relaxed);
        return Ok(None);
    }

    // Phase 2: decode metadata (expensive zstd + protobuf) — only for potential matches.
    let t1 = Instant::now();
    let meta = ptx
        .meta_data
        .as_deref()
        .map(|compressed| {
            ZSTD_STATE.with(|state| {
                let (ref mut decompressor, ref mut buf) = *state.borrow_mut();
                harpoon_solana::decode_metadata_reuse(compressed, decompressor, buf)
            })
        })
        .transpose()
        .ok()
        .flatten()
        .flatten();
    timing.record(
        &timing.meta_decode_ns,
        &timing.meta_decode_samples,
        t1.elapsed(),
        sample,
    );

    let account_keys = harpoon_solana::resolve_full_account_keys(&tx, meta.as_ref());

    // If outer didn't match, we need to verify via inner instructions with full keys.
    if !outer_matched {
        let matched = harpoon_solana::matches_programs(
            &target_slice,
            &account_keys,
            outer_instructions,
            meta.as_ref(),
        );
        if !matched {
            return Ok(None);
        }
    }

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

    let instruction_records: Vec<InstructionRecord> = instructions
        .iter()
        .map(|ix| {
            let mut rec = InstructionRecord {
                program_id: ix.program_id.clone(),
                data: ix.data.clone(),
                accounts: ix.accounts.clone(),
            };

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
        log_messages: meta.as_ref().and_then(|m| m.log_messages.clone()),
        err: meta
            .as_ref()
            .and_then(|m| m.status.as_ref().err().map(|e| format!("{e:?}"))),
        instructions: instruction_records,
    };

    timing.record(
        &timing.build_record_ns,
        &timing.build_record_samples,
        t3.elapsed(),
        sample,
    );
    Ok(Some(record))
}

// ===========================================================================
// Extract pipeline processing
// ===========================================================================

#[allow(clippy::too_many_arguments)]
fn process_extract_chunks(
    prepared: &mut Vec<PreparedTx>,
    target_programs: &HashSet<Pubkey>,
    idl_decoder: &IdlDecoder,
    extract_mode: ExtractMode,
    stats: &PipelineStats,
    timing: &TimingStats,
    buffer: &mut Vec<FlatRecord>,
    writer_tx: &SyncSender<Vec<FlatRecord>>,
    batch_size: usize,
) -> anyhow::Result<()> {
    while prepared.len() >= RAYON_CHUNK_SIZE {
        let chunk: Vec<PreparedTx> = prepared.drain(..RAYON_CHUNK_SIZE).collect();
        let records =
            process_extract_batch(chunk, target_programs, idl_decoder, extract_mode, stats, timing);
        buffer.extend(records);
        flush_flat_if_full(buffer, writer_tx, batch_size)?;
    }

    if !prepared.is_empty() {
        let chunk: Vec<PreparedTx> = std::mem::take(prepared);
        let records =
            process_extract_batch(chunk, target_programs, idl_decoder, extract_mode, stats, timing);
        buffer.extend(records);
        flush_flat_if_full(buffer, writer_tx, batch_size)?;
    }

    Ok(())
}

fn flush_flat_if_full(
    buffer: &mut Vec<FlatRecord>,
    writer_tx: &SyncSender<Vec<FlatRecord>>,
    batch_size: usize,
) -> anyhow::Result<()> {
    while buffer.len() >= batch_size {
        let batch: Vec<FlatRecord> = buffer.drain(..batch_size).collect();
        writer_tx
            .send(batch)
            .map_err(|_| anyhow!("writer thread gone"))?;
    }
    Ok(())
}

fn process_extract_batch(
    prepared: Vec<PreparedTx>,
    target_programs: &HashSet<Pubkey>,
    idl_decoder: &IdlDecoder,
    extract_mode: ExtractMode,
    stats: &PipelineStats,
    timing: &TimingStats,
) -> Vec<FlatRecord> {
    prepared
        .into_par_iter()
        .flat_map(|ptx| {
            match process_single_tx_extract(
                &ptx,
                target_programs,
                idl_decoder,
                extract_mode,
                stats,
                timing,
            ) {
                Ok(records) => {
                    if !records.is_empty() {
                        stats.matching_transactions.fetch_add(1, Ordering::Relaxed);
                    }
                    records
                }
                Err(_) => {
                    stats.decode_errors.fetch_add(1, Ordering::Relaxed);
                    Vec::new()
                }
            }
        })
        .collect()
}

fn process_single_tx_extract(
    ptx: &PreparedTx,
    target_programs: &HashSet<Pubkey>,
    idl_decoder: &IdlDecoder,
    extract_mode: ExtractMode,
    stats: &PipelineStats,
    timing: &TimingStats,
) -> anyhow::Result<Vec<FlatRecord>> {
    let sample = timing.should_sample();

    // Phase 1: decode transaction (cheap) and check outer instructions.
    let t0 = Instant::now();
    let tx =
        harpoon_solana::decode_transaction(&ptx.tx_data).context("tx decode")?;
    timing.record(
        &timing.tx_decode_ns,
        &timing.tx_decode_samples,
        t0.elapsed(),
        sample,
    );

    let static_keys = match &tx.message {
        VersionedMessage::Legacy(msg) => &msg.account_keys,
        VersionedMessage::V0(msg) => &msg.account_keys,
    };
    let outer_instructions = match &tx.message {
        VersionedMessage::Legacy(msg) => &msg.instructions,
        VersionedMessage::V0(msg) => &msg.instructions,
    };

    let t2 = Instant::now();
    let target_slice: Vec<Pubkey> = target_programs.iter().copied().collect();
    let outer_matched =
        harpoon_solana::matches_outer_programs(&target_slice, static_keys, outer_instructions);
    timing.record(
        &timing.match_check_ns,
        &timing.match_check_samples,
        t2.elapsed(),
        sample,
    );

    if !outer_matched && !harpoon_solana::could_match_programs(&target_slice, static_keys) {
        stats.metadata_skipped.fetch_add(1, Ordering::Relaxed);
        return Ok(Vec::new());
    }

    // Phase 2: decode metadata (expensive).
    let t1 = Instant::now();
    let meta = ptx
        .meta_data
        .as_deref()
        .map(|compressed| {
            ZSTD_STATE.with(|state| {
                let (ref mut decompressor, ref mut buf) = *state.borrow_mut();
                harpoon_solana::decode_metadata_reuse(compressed, decompressor, buf)
            })
        })
        .transpose()
        .ok()
        .flatten()
        .flatten();
    timing.record(
        &timing.meta_decode_ns,
        &timing.meta_decode_samples,
        t1.elapsed(),
        sample,
    );

    let account_keys = harpoon_solana::resolve_full_account_keys(&tx, meta.as_ref());

    if !outer_matched {
        let matched = harpoon_solana::matches_programs(
            &target_slice,
            &account_keys,
            outer_instructions,
            meta.as_ref(),
        );
        if !matched {
            return Ok(Vec::new());
        }
    }

    let signature = tx
        .signatures
        .first()
        .map(|s| s.to_string())
        .unwrap_or_default();

    match extract_mode {
        ExtractMode::Events => {
            if let Some(ref m) = meta {
                if let Some(ref logs) = m.log_messages {
                    return Ok(extract_events_from_logs(
                        logs,
                        idl_decoder,
                        ptx.slot,
                        ptx.block_time,
                        &signature,
                    ));
                }
            }
            Ok(Vec::new())
        }
        ExtractMode::Instructions => {
            let instructions =
                harpoon_solana::extract_instructions(&account_keys, &tx, meta.as_ref());
            Ok(extract_decoded_instructions(
                &instructions,
                idl_decoder,
                ptx.slot,
                ptx.block_time,
                &signature,
            ))
        }
        ExtractMode::Raw => unreachable!(),
    }
}

/// Parse "Program data: <base64>" from log messages, decode events via IDL.
fn extract_events_from_logs(
    logs: &[String],
    idl_decoder: &IdlDecoder,
    slot: u64,
    block_time: Option<i64>,
    signature: &str,
) -> Vec<FlatRecord> {
    let mut program_stack: Vec<&str> = Vec::new();
    let mut records = Vec::new();

    for line in logs {
        // Track program invocation stack for program_id attribution
        if let Some(rest) = line.strip_prefix("Program ") {
            if let Some(idx) = rest.find(' ') {
                let prog_id = &rest[..idx];
                let remainder = &rest[idx + 1..];
                if remainder.starts_with("invoke") {
                    program_stack.push(prog_id);
                } else if remainder.starts_with("success") || remainder.starts_with("failed") {
                    program_stack.pop();
                }
            }
        }

        if let Some(b64_data) = line.strip_prefix("Program data: ") {
            if let Ok(bytes) = BASE64_STANDARD.decode(b64_data.trim()) {
                if let Some(Ok(decoded)) = idl_decoder.try_decode_event(&bytes) {
                    let program_id = program_stack
                        .last()
                        .map(|s| (*s).to_string())
                        .unwrap_or_default();
                    let fields = match decoded.data {
                        serde_json::Value::Object(map) => map,
                        _ => serde_json::Map::new(),
                    };
                    records.push(FlatRecord {
                        slot,
                        block_time,
                        signature: signature.to_string(),
                        name: decoded.name,
                        program_id,
                        fields,
                    });
                }
            }
        }
    }

    records
}

/// Decode instructions via IDL, returning flat records.
fn extract_decoded_instructions(
    instructions: &[harpoon_solana::ParsedInstruction],
    idl_decoder: &IdlDecoder,
    slot: u64,
    block_time: Option<i64>,
    signature: &str,
) -> Vec<FlatRecord> {
    let mut records = Vec::new();

    for ix in instructions {
        if let Ok(raw_bytes) = bs58::decode(&ix.data).into_vec() {
            if let Some(Ok(decoded)) = idl_decoder.try_decode_instruction(&raw_bytes) {
                let fields = match decoded.data {
                    serde_json::Value::Object(map) => map,
                    _ => serde_json::Map::new(),
                };
                records.push(FlatRecord {
                    slot,
                    block_time,
                    signature: signature.to_string(),
                    name: decoded.name,
                    program_id: ix.program_id.clone(),
                    fields,
                });
            }
        }
    }

    records
}

// ===========================================================================
// Summary
// ===========================================================================

fn print_summary(stats: &PipelineStats, start_time: Instant) {
    let elapsed = start_time.elapsed();
    let total = stats.total_transactions.load(Ordering::Relaxed);
    let matched = stats.matching_transactions.load(Ordering::Relaxed);
    let metadata_errors = stats.metadata_errors.load(Ordering::Relaxed);
    let decode_errors = stats.decode_errors.load(Ordering::Relaxed);
    let metadata_skipped = stats.metadata_skipped.load(Ordering::Relaxed);
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
        "  metadata skipped:    {}",
        crate::stats::format_number(metadata_skipped)
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
