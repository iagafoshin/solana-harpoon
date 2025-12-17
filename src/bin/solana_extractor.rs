use {
    anyhow::{anyhow, Context},
    arrow_array::{
        ArrayRef, RecordBatch,
        builder::{
            ArrayBuilder, Int64Builder, ListBuilder, StringBuilder, StructBuilder, UInt32Builder,
            UInt64Builder,
        },
    },
    arrow_schema::{DataType, Field, Fields, Schema, SchemaRef},
    bs58,
    bytes::Bytes,
    clap::Parser,
    futures_util::TryStreamExt,
    parquet::{
        arrow::ArrowWriter,
        basic::{Compression, ZstdLevel},
        file::properties::WriterProperties,
    },
    prost::Message,
    rayon::prelude::*,
    reqwest::Client,
    serde::Serialize,
    solana_sdk::{
        instruction::CompiledInstruction, message::VersionedMessage, pubkey::Pubkey,
        transaction::VersionedTransaction,
    },
    solana_storage_proto::{convert::generated, StoredTransactionStatusMeta},
    solana_transaction_status::{TransactionStatusMeta, TransactionTokenBalance},
    std::{
        borrow::Cow,
        collections::{BTreeMap, HashSet},
        convert::TryFrom,
        path::PathBuf,
        process::Command,
        str::FromStr,
        sync::{
            atomic::{AtomicU64, Ordering},
            mpsc::sync_channel,
            Arc,
        },
        time::{Duration, Instant},
    },
    tikv_jemallocator::Jemalloc,
    tokio::{
        fs, fs::File,
        io::{self, AsyncRead, BufReader},
    },
    tokio_util::io::StreamReader,
    yellowstone_faithful_car_parser::node::{Node, NodeReader, Nodes},
};

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const DEFAULT_PROGRAMS: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PROGRESS_INTERVAL: u64 = 10_000;
const BATCH_SIZE: usize = 50_000;
const RAYON_CHUNK_SIZE: usize = 5_000;
const TIMING_SAMPLE_RATE: u64 = 256;

#[derive(Debug, Parser, Clone)]
#[clap(author, version, about = "generic Solana CAR extractor")]
struct Args {
    /// Latest epoch number to process
    #[clap(long, default_value_t = 881)]
    pub latest_epoch: u64,

    /// Number of epochs to process (walking backwards)
    #[clap(long, default_value_t = 5)]
    pub num_epochs: u64,

    /// Directory where CAR and Parquet files will be stored
    #[clap(long, default_value = "./data")]
    pub data_dir: PathBuf,

    /// Target program ids (comma-separated)
    #[clap(long)]
    pub program: Option<String>,

    /// Stream-download CAR files directly over HTTP without saving to disk
    #[clap(long)]
    pub stream_download: bool,

    /// Keep downloaded CAR files on disk
    #[clap(long)]
    pub keep_car: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    fs::create_dir_all(&args.data_dir)
        .await
        .context("failed to create data directory")?;

    let program_list = args.program.as_deref().unwrap_or(DEFAULT_PROGRAMS);
    let target_programs: Vec<Pubkey> = program_list
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(Pubkey::from_str)
        .collect::<Result<Vec<_>, _>>()
        .context("failed to parse program ids")?;
    if target_programs.is_empty() {
        return Err(anyhow!("no program ids provided"));
    }
    let target_programs = Arc::new(target_programs.into_iter().collect::<HashSet<Pubkey>>());

    if args.num_epochs == 0 {
        return Ok(());
    }

    let available_epochs = args.latest_epoch.saturating_add(1);
    let epochs_to_process = args.num_epochs.min(available_epochs);
    if epochs_to_process < args.num_epochs {
        eprintln!(
            "[warn] requested num_epochs={} but only {} epochs available from latest_epoch={}; processing {} epochs",
            args.num_epochs,
            available_epochs,
            args.latest_epoch,
            epochs_to_process
        );
    }

    eprintln!(
        "Starting pipeline for programs [{}] across {} epochs (latest={})",
        program_list, epochs_to_process, args.latest_epoch
    );

    for offset in 0..epochs_to_process {
        let epoch = args.latest_epoch - offset;
        let car_filename = format!("epoch-{epoch}.car");
        let temp_filename = format!("{car_filename}.part");
        let parquet_filename = format!("whales-epoch-{epoch}.parquet");
        let car_path = args.data_dir.join(&car_filename);
        let temp_car_path = args.data_dir.join(&temp_filename);
        let output_path = args.data_dir.join(&parquet_filename);

        if args.stream_download {
            let url = format!("https://files.old-faithful.net/{epoch}/epoch-{epoch}.car");
            eprintln!(
                "Streaming epoch {epoch} from {url} -> {:?}",
                output_path
            );
            process_network_stream(
                &url,
                output_path,
                target_programs.clone(),
            )
            .await?;
        } else {
            if !car_path.exists() {
                let url = format!("https://files.old-faithful.net/{epoch}/epoch-{epoch}.car");
                eprintln!("Downloading epoch {epoch} from {url}");
                let status = Command::new("aria2c")
                    .arg("-x")
                    .arg("16")
                    .arg("-s")
                    .arg("16")
                    .arg("-k")
                    .arg("4M")
                    .arg("--auto-file-renaming=false")
                    .arg("--allow-overwrite=true")
                    .arg("-c")
                    .arg("-o")
                    .arg(&temp_filename)
                    .arg(&url)
                    .current_dir(&args.data_dir)
                    .status()
                    .context("failed to spawn aria2c")?;
                if !status.success() {
                    return Err(anyhow!("aria2c failed for epoch {epoch}"));
                }
                std::fs::rename(&temp_car_path, &car_path).with_context(|| {
                    format!(
                        "failed to rename downloaded temp file {:?} to {:?}",
                        temp_car_path, car_path
                    )
                })?;
            }

            eprintln!(
                "Processing CAR {:?} -> {:?} for programs [{}]",
                car_path,
                output_path,
                target_programs
                    .iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            );
            let file = File::open(&car_path)
                .await
                .context("failed to open CAR file")?;
            process_stream(
                Box::new(file),
                output_path,
                target_programs.clone(),
            )
            .await?;
            if !args.keep_car {
                fs::remove_file(&car_path)
                    .await
                    .context("failed to remove downloaded CAR")?;
            }
        }
    }

    Ok(())
}

async fn process_stream(
    input: Box<dyn AsyncRead + Unpin + Send>,
    output_path: PathBuf,
    target_programs: Arc<HashSet<Pubkey>>,
) -> anyhow::Result<()> {
    let mut reader = NodeReader::new(BufReader::new(input));

    let output_file = std::fs::File::create(&output_path)
        .with_context(|| format!("failed to create {:?}", output_path))?;
    let writer_props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let schema = build_schema();
    let writer = ArrowWriter::try_new(output_file, schema.clone(), Some(writer_props))
        .context("failed to create parquet writer")?;
    let timing = Arc::new(TimingStats::default());
    let (batch_tx, batch_rx) = sync_channel::<Vec<WhalesV1Record>>(4);
    // Dedicated writer thread owns the ArrowWriter to avoid blocking the async runtime.
    let writer_handle = std::thread::spawn({
        let schema = schema.clone();
        let timing = timing.clone();
        move || -> anyhow::Result<()> {
            let mut writer = writer;
            for batch in batch_rx.iter() {
                let start = Instant::now();
                let batch_len = batch.len();
                flush_batch_sync(&schema, &mut writer, batch)
                    .with_context(|| format!("parquet_write failed for batch size {batch_len}"))?;
                timing.record(
                    &timing.parquet_write_ns,
                    &timing.parquet_write_samples,
                    start.elapsed(),
                    true,
                );
            }
            writer
                .close()
                .with_context(|| "parquet_close failed while finalizing writer".to_string())?;
            Ok(())
        }
    });
    let mut buffer: Vec<WhalesV1Record> = Vec::new();

    let start_time = Instant::now();
    let stats = Stats::default();
    let mut first_slot = None;
    let mut last_slot = None;
    loop {
        let nodes = Nodes::read_until_block(&mut reader).await?;
        if nodes.nodes.is_empty() {
            break;
        }

        let block_time = nodes.nodes.values().find_map(|node| match node {
            Node::Block(block) => i64::try_from(block.meta.blocktime).ok(),
            _ => None,
        });
        let mut prepared_txs = Vec::new();

        for node in nodes.nodes.values() {
            match node {
                Node::Transaction(frame) => {
                    if first_slot.is_none() {
                        first_slot = Some(frame.slot);
                    }
                    last_slot = Some(frame.slot);
                    let total = stats.total_transactions.fetch_add(1, Ordering::Relaxed) + 1;
                    if total % PROGRESS_INTERVAL == 0 {
                        print_progress(&stats, last_slot, start_time);
                    }

                    let meta_data = match nodes.reassemble_dataframes(&frame.metadata) {
                        Ok(bytes) if bytes.is_empty() => None,
                        Ok(bytes) => Some(Bytes::from(bytes)),
                        Err(err) => {
                            eprintln!(
                                "[warn] slot={} failed to reassemble tx metadata: {err}",
                                frame.slot
                            );
                            stats.metadata_errors.fetch_add(1, Ordering::Relaxed);
                            None
                        }
                    };
                    prepared_txs.push(PreparedTx {
                        slot: frame.slot,
                        block_time,
                        // Cloning is required because `frame` is borrowed from `nodes`; we cannot take ownership of the buffer.
                        tx_data: Bytes::from(frame.data.data.clone()),
                        meta_data,
                    });
                    if prepared_txs.len() >= RAYON_CHUNK_SIZE {
                        let chunk: Vec<_> = prepared_txs.drain(..RAYON_CHUNK_SIZE).collect();
                        let records = process_prepared_transactions(
                            chunk,
                            &target_programs,
                            &stats,
                            timing.as_ref(),
                        );
                        buffer.extend(records);
                        while buffer.len() >= BATCH_SIZE {
                            let batch: Vec<_> = buffer.drain(..BATCH_SIZE).collect();
                            batch_tx
                                .send(batch)
                                .map_err(|_| {
                                    anyhow!("failed to send parquet batch to writer thread")
                                })?;
                        }
                    }
                }
                Node::Block(_) => {
                    stats.total_blocks.fetch_add(1, Ordering::Relaxed);
                }
                Node::Rewards(_) | Node::Entry(_) | Node::Subset(_) | Node::Epoch(_) | Node::DataFrame(_) => (),
            }
        }

        // Backpressure: process prepared txs in bounded chunks to avoid ballooning memory when many match.
        while prepared_txs.len() >= RAYON_CHUNK_SIZE {
            let chunk: Vec<_> = prepared_txs.drain(..RAYON_CHUNK_SIZE).collect();
            let records =
                process_prepared_transactions(chunk, &target_programs, &stats, timing.as_ref());
            buffer.extend(records);
            while buffer.len() >= BATCH_SIZE {
                let batch: Vec<_> = buffer.drain(..BATCH_SIZE).collect();
                batch_tx
                    .send(batch)
                    .map_err(|_| anyhow!("failed to send parquet batch to writer thread"))?;
            }
        }
        if !prepared_txs.is_empty() {
            let chunk: Vec<_> = prepared_txs.drain(..).collect();
            let records =
                process_prepared_transactions(chunk, &target_programs, &stats, timing.as_ref());
            buffer.extend(records);
            while buffer.len() >= BATCH_SIZE {
                let batch: Vec<_> = buffer.drain(..BATCH_SIZE).collect();
                batch_tx
                    .send(batch)
                    .map_err(|_| anyhow!("failed to send parquet batch to writer thread"))?;
            }
        }
    }

    if !buffer.is_empty() {
        let batch = std::mem::take(&mut buffer);
        batch_tx
            .send(batch)
            .map_err(|_| anyhow!("failed to send final parquet batch to writer thread"))?;
    }
    drop(batch_tx); // signal completion
    let writer_result = writer_handle
        .join()
        .map_err(|_| anyhow!("writer thread panicked"))?;
    writer_result?;

    let elapsed = start_time.elapsed();
    print_final_summary(&stats, first_slot, last_slot, elapsed);
    timing.print_summary();

    Ok(())
}

async fn process_network_stream(
    url: &str,
    output_path: PathBuf,
    target_programs: Arc<HashSet<Pubkey>>,
) -> anyhow::Result<()> {
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(6 * 60 * 60))
        .build()
        .context("failed to build http client")?;
    let response = client.get(url).send().await.context("http request failed")?;
    let status = response.status();
    if !status.is_success() {
        return Err(anyhow!("http request failed with status {status} for {url}"));
    }

    let stream = response.bytes_stream().map_err(|err| io::Error::new(io::ErrorKind::Other, err));
    let reader = StreamReader::new(stream);
    process_stream(
        Box::new(reader),
        output_path,
        target_programs,
    )
    .await
}

enum DecodedData<B, P> {
    Bincode(B),
    Protobuf(P),
}

fn decode_protobuf_bincode<B, P>(kind: &str, bytes: &[u8]) -> anyhow::Result<DecodedData<B, P>>
where
    B: serde::de::DeserializeOwned,
    P: Message + Default,
{
    match P::decode(bytes) {
        Ok(value) => Ok(DecodedData::Protobuf(value)),
        Err(_) => bincode::deserialize::<B>(bytes)
            .map(DecodedData::Bincode)
            .with_context(|| format!("failed to decode {kind} with protobuf/bincode")),
    }
}

#[derive(Serialize)]
struct ParsedInstruction {
    program_id: String,
    data: String,
    accounts: Vec<String>,
}

#[derive(Serialize)]
struct WhalesV1Record {
    slot: u64,
    block_time: Option<i64>,
    signatures: Vec<String>,
    accounts: Vec<String>,
    program_ids: Vec<String>,
    fee: u64,
    account_balance_deltas: Vec<AccountBalanceDelta>,
    token_balance_deltas: Vec<TokenBalanceDelta>,
    log_messages: Option<Vec<String>>,
    err: Option<String>,
    instructions: Vec<ParsedInstruction>,
}

#[derive(Serialize)]
struct AccountBalanceDelta {
    account: String,
    pre_lamports: u64,
    post_lamports: u64,
    delta_lamports: i64,
}

#[derive(Serialize)]
struct TokenBalanceDelta {
    mint: String,
    owner: String,
    account_index: u32,
    pre_ui_amount: Option<String>,
    post_ui_amount: Option<String>,
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

struct Stats {
    total_transactions: AtomicU64,
    matching_transactions: AtomicU64,
    total_blocks: AtomicU64,
    metadata_errors: AtomicU64,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            total_transactions: AtomicU64::new(0),
            matching_transactions: AtomicU64::new(0),
            total_blocks: AtomicU64::new(0),
            metadata_errors: AtomicU64::new(0),
        }
    }
}

fn print_progress(stats: &Stats, last_slot: Option<u64>, start_time: Instant) {
    let total = stats.total_transactions.load(Ordering::Relaxed);
    let matched = stats.matching_transactions.load(Ordering::Relaxed);
    let blocks = stats.total_blocks.load(Ordering::Relaxed);
    let metadata_errors = stats.metadata_errors.load(Ordering::Relaxed);
    let slot = last_slot.unwrap_or(0);
    let elapsed = start_time.elapsed();
    let speed = if elapsed.as_secs_f64() > 0.0 {
        (total as f64 / elapsed.as_secs_f64()) as u64
    } else {
        0
    };

    eprintln!(
        "[progress] total={} matched={} blocks={} slot={} speed={} tx/s metadata_errors={}",
        format_number(total),
        format_number(matched),
        format_number(blocks),
        format_number(slot),
        format_number(speed),
        format_number(metadata_errors)
    );
}

struct PreparedTx {
    slot: u64,
    block_time: Option<i64>,
    tx_data: Bytes,
    meta_data: Option<Bytes>,
}

#[derive(Default)]
struct TimingStats {
    sample_counter: AtomicU64,
    tx_decode_ns: AtomicU64,
    tx_decode_samples: AtomicU64,
    meta_decode_ns: AtomicU64,
    meta_decode_samples: AtomicU64,
    match_check_ns: AtomicU64,
    match_check_samples: AtomicU64,
    build_record_ns: AtomicU64,
    build_record_samples: AtomicU64,
    parquet_write_ns: AtomicU64,
    parquet_write_samples: AtomicU64,
}

impl TimingStats {
    fn should_sample(&self) -> bool {
        self.sample_counter.fetch_add(1, Ordering::Relaxed) % TIMING_SAMPLE_RATE == 0
    }

    fn record(&self, total_ns: &AtomicU64, samples: &AtomicU64, duration: std::time::Duration, do_sample: bool) {
        if do_sample {
            let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
            total_ns.fetch_add(nanos, Ordering::Relaxed);
            samples.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn print_summary(&self) {
        let stages = [
            ("tx_decode", &self.tx_decode_ns, &self.tx_decode_samples),
            ("meta_decode", &self.meta_decode_ns, &self.meta_decode_samples),
            ("match_check", &self.match_check_ns, &self.match_check_samples),
            ("build_record", &self.build_record_ns, &self.build_record_samples),
            ("parquet_write", &self.parquet_write_ns, &self.parquet_write_samples),
        ];
        eprintln!("[timing]");
        for (label, total, count) in stages {
            let c = count.load(Ordering::Relaxed);
            let ns = total.load(Ordering::Relaxed);
            if c == 0 {
                eprintln!("  {:<14} total={:>10.3} ms avg={:>8} samples=0", label, 0.0, "n/a");
            } else {
                let avg_ns = ns / c;
                let ms = ns as f64 / 1_000_000.0;
                eprintln!(
                    "  {:<14} total={:>10.3} ms avg={:>8.3} µs samples={}",
                    label,
                    ms,
                    avg_ns as f64 / 1_000.0,
                    c
                );
            }
        }
    }
}

fn process_prepared_transactions(
    prepared: Vec<PreparedTx>,
    target_programs: &HashSet<Pubkey>,
    stats: &Stats,
    timing: &TimingStats,
) -> Vec<WhalesV1Record> {
    prepared
        .into_par_iter()
        .filter_map(|tx| {
            let slot = tx.slot;
            match process_prepared_tx(tx, target_programs, stats, timing) {
                Ok(Some(record)) => {
                    stats.matching_transactions.fetch_add(1, Ordering::Relaxed);
                    Some(record)
                }
                Ok(None) => None,
                Err(err) => {
                    eprintln!("[warn] slot={slot} failed to process tx: {err:?}");
                    None
                }
            }
        })
        .collect()
}

fn process_prepared_tx(
    prepared: PreparedTx,
    target_programs: &HashSet<Pubkey>,
    stats: &Stats,
    timing: &TimingStats,
) -> anyhow::Result<Option<WhalesV1Record>> {
    let sample = timing.should_sample();
    let tx_decode_start = Instant::now();
    let tx: VersionedTransaction =
        bincode::deserialize(prepared.tx_data.as_ref()).context("failed to parse tx")?;
    timing.record(
        &timing.tx_decode_ns,
        &timing.tx_decode_samples,
        tx_decode_start.elapsed(),
        sample,
    );

    let (account_keys, instructions) = match &tx.message {
        VersionedMessage::Legacy(msg) => (&msg.account_keys, &msg.instructions),
        VersionedMessage::V0(msg) => (&msg.account_keys, &msg.instructions),
    };

    let match_check_start = Instant::now();
    let outer_match = matches_outer(target_programs, account_keys, instructions);

    let mut meta: Option<TransactionStatusMeta> = None;
    if !outer_match {
        let Some(meta_bytes) = prepared.meta_data.as_ref() else {
            return Ok(None);
        };
        if !meta_bytes.is_empty() {
            let meta_decode_start = Instant::now();
            match try_decode_meta(meta_bytes.as_ref(), prepared.slot, stats) {
                Ok(Some(decoded)) => meta = Some(decoded),
                Ok(None) => {}
                Err(_) => return Ok(None),
            }
            timing.record(
                &timing.meta_decode_ns,
                &timing.meta_decode_samples,
                meta_decode_start.elapsed(),
                sample,
            );
        }
    } else if let Some(meta_bytes) = prepared.meta_data.as_ref() {
        if !meta_bytes.is_empty() {
            let meta_decode_start = Instant::now();
            match try_decode_meta(meta_bytes.as_ref(), prepared.slot, stats) {
                Ok(Some(decoded)) => meta = Some(decoded),
                Ok(None) => {}
                Err(_) => {
                    // keep meta = None; outer match must still emit a record
                }
            }
            timing.record(
                &timing.meta_decode_ns,
                &timing.meta_decode_samples,
                meta_decode_start.elapsed(),
                sample,
            );
        }
    }

    let mut full_account_keys: Vec<Pubkey> = account_keys.clone();
    if let (VersionedMessage::V0(_), Some(meta)) = (&tx.message, meta.as_ref()) {
        full_account_keys.extend(meta.loaded_addresses.writable.iter().cloned());
        full_account_keys.extend(meta.loaded_addresses.readonly.iter().cloned());
    }

    let inner_instruction_sets = meta
        .as_ref()
        .and_then(|meta| meta.inner_instructions.as_ref());

    let match_result = if outer_match {
        true
    } else {
        matches_inner(target_programs, &full_account_keys, inner_instruction_sets)
    };
    timing.record(
        &timing.match_check_ns,
        &timing.match_check_samples,
        match_check_start.elapsed(),
        sample,
    );
    if !match_result {
        return Ok(None);
    }

    let mut program_ids: Vec<Pubkey> = Vec::new();
    for ix in instructions {
        if let Some(key) = resolve_program_key(&full_account_keys, ix.program_id_index) {
            program_ids.push(key);
        }
    }
    if let Some(inner_sets) = inner_instruction_sets {
        for inner in inner_sets {
            for ix in &inner.instructions {
                if let Some(key) =
                    resolve_program_key(&full_account_keys, ix.instruction.program_id_index)
                {
                    program_ids.push(key);
                }
            }
        }
    }
    program_ids.sort_unstable();
    program_ids.dedup();

    let build_start = Instant::now();
    let parsed_instructions =
        build_parsed_instructions(&full_account_keys, instructions, inner_instruction_sets);

    let account_balance_deltas = meta
        .as_ref()
        .map(|meta| build_account_balance_deltas(meta, &full_account_keys))
        .unwrap_or_default();
    let token_balance_deltas = meta
        .as_ref()
        .map(build_token_balance_deltas)
        .unwrap_or_default();
    let fee = meta.as_ref().map(|meta| meta.fee).unwrap_or_default();
    let record = WhalesV1Record {
        slot: prepared.slot,
        block_time: prepared.block_time,
        signatures: tx.signatures.iter().map(|sig| sig.to_string()).collect(),
        accounts: full_account_keys
            .iter()
            .map(|key| key.to_string())
            .collect(),
        program_ids: program_ids.into_iter().map(|key| key.to_string()).collect(),
        fee,
        account_balance_deltas,
        token_balance_deltas,
        log_messages: meta
            .as_ref()
            .and_then(|meta| meta.log_messages.as_ref().cloned()),
        err: meta
            .as_ref()
            .and_then(|meta| meta.status.as_ref().err().map(|e| format!("{e:?}"))),
        instructions: parsed_instructions,
    };
    timing.record(
        &timing.build_record_ns,
        &timing.build_record_samples,
        build_start.elapsed(),
        sample,
    );

    Ok(Some(record))
}

fn decode_transaction_status_meta(buffer: &[u8]) -> anyhow::Result<TransactionStatusMeta> {
    match decode_protobuf_bincode::<StoredTransactionStatusMeta, generated::TransactionStatusMeta>(
        "tx metadata",
        buffer,
    )? {
        DecodedData::Bincode(value) => Ok(TransactionStatusMeta::from(value)),
        DecodedData::Protobuf(value) => TransactionStatusMeta::try_from(value)
            .map_err(|err| anyhow!("failed to convert proto tx metadata: {err}")),
    }
}

fn try_decode_meta(
    meta_data: &[u8],
    slot: u64,
    stats: &Stats,
) -> anyhow::Result<Option<TransactionStatusMeta>> {
    let buffer = zstd::decode_all(meta_data);
    let buffer = match buffer {
        Ok(buffer) => buffer,
        Err(err) => {
            eprintln!("[warn] failed to decompress tx metadata at slot {}: {err}", slot);
            stats.metadata_errors.fetch_add(1, Ordering::Relaxed);
            return Err(anyhow!("metadata decompress failed"));
        }
    };
    if buffer.is_empty() {
        return Ok(None);
    }
    match decode_transaction_status_meta(&buffer) {
        Ok(decoded) => Ok(Some(decoded)),
        Err(err) => {
            eprintln!("[warn] failed to decode tx metadata at slot {}: {err}", slot);
            stats.metadata_errors.fetch_add(1, Ordering::Relaxed);
            Err(err)
        }
    }
}

fn program_matches(
    program_id_index: u8,
    account_keys: &[Pubkey],
    target_programs: &HashSet<Pubkey>,
) -> bool {
    account_keys
        .get(program_id_index as usize)
        .map_or(false, |key| target_programs.contains(key))
}

fn matches_outer(
    target_programs: &HashSet<Pubkey>,
    account_keys: &[Pubkey],
    instructions: &[CompiledInstruction],
) -> bool {
    instructions
        .iter()
        .any(|ix| program_matches(ix.program_id_index, account_keys, target_programs))
}

fn matches_inner(
    target_programs: &HashSet<Pubkey>,
    account_keys: &[Pubkey],
    inner_instruction_sets: Option<&Vec<solana_transaction_status::InnerInstructions>>,
) -> bool {
    let Some(inner_sets) = inner_instruction_sets else {
        return false;
    };
    for inner in inner_sets {
        for ix in &inner.instructions {
            if program_matches(ix.instruction.program_id_index, account_keys, target_programs) {
                return true;
            }
        }
    }
    false
}

fn resolve_program_id(account_keys: &[Pubkey], program_id_index: u8) -> String {
    resolve_program_key(account_keys, program_id_index)
        .map(|key| key.to_string())
        .unwrap_or_else(|| format!("UNKNOWN_PROGRAM_INDEX_{program_id_index}"))
}

fn resolve_program_key(account_keys: &[Pubkey], program_id_index: u8) -> Option<Pubkey> {
    account_keys
        .get(program_id_index as usize)
        .copied()
}

fn build_parsed_instructions(
    account_keys: &[Pubkey],
    instructions: &[CompiledInstruction],
    inner_instruction_sets: Option<&Vec<solana_transaction_status::InnerInstructions>>,
) -> Vec<ParsedInstruction> {
    let mut result = Vec::new();
    for ix in instructions {
        result.push(resolve_instruction(ix, account_keys));
    }
    if let Some(inner_sets) = inner_instruction_sets {
        for inner in inner_sets {
            for ix in &inner.instructions {
                result.push(resolve_instruction(&ix.instruction, account_keys));
            }
        }
    }
    result
}

fn resolve_instruction(ix: &CompiledInstruction, account_keys: &[Pubkey]) -> ParsedInstruction {
    let program_id = resolve_program_id(account_keys, ix.program_id_index);
    let accounts = ix
        .accounts
        .iter()
        .map(|idx| resolve_program_id(account_keys, *idx))
        .collect();
    let data = bs58::encode(&ix.data).into_string();
    ParsedInstruction {
        program_id,
        data,
        accounts,
    }
}

fn build_account_balance_deltas(
    meta: &TransactionStatusMeta,
    account_keys: &[Pubkey],
) -> Vec<AccountBalanceDelta> {
    meta.pre_balances
        .iter()
        .zip(meta.post_balances.iter())
        .enumerate()
        .map(|(idx, (pre, post))| AccountBalanceDelta {
            account: account_keys
                .get(idx)
                .map(|key| key.to_string())
                .unwrap_or_else(|| format!("UNKNOWN_ACCOUNT_INDEX_{idx}")),
            pre_lamports: *pre,
            post_lamports: *post,
            delta_lamports: *post as i64 - *pre as i64,
        })
        .collect()
}

fn build_token_balance_deltas(meta: &TransactionStatusMeta) -> Vec<TokenBalanceDelta> {
    fn owner_string(balance: &TransactionTokenBalance) -> Cow<'_, str> {
        // Handles both String and Option<String> across crate versions
        balance.owner.owner_string()
    }

    fn apply_balances<'a>(
        map: &mut BTreeMap<(String, String, u8), TokenBalanceDelta>,
        balances: impl Iterator<Item = &'a TransactionTokenBalance>,
        is_post: bool,
    ) {
        for balance in balances {
            let key = (
                balance.mint.clone(),
                owner_string(balance).into_owned(),
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

    let mut map: BTreeMap<(String, String, u8), TokenBalanceDelta> = BTreeMap::new();
    if let Some(pre) = meta.pre_token_balances.as_ref() {
        apply_balances(&mut map, pre.iter(), false);
    }
    if let Some(post) = meta.post_token_balances.as_ref() {
        apply_balances(&mut map, post.iter(), true);
    }
    map.into_values().collect()
}

fn print_final_summary(
    stats: &Stats,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    elapsed: std::time::Duration,
) {
    let total = stats.total_transactions.load(Ordering::Relaxed);
    let matched = stats.matching_transactions.load(Ordering::Relaxed);
    let metadata_errors = stats.metadata_errors.load(Ordering::Relaxed);
    let blocks = stats.total_blocks.load(Ordering::Relaxed);
    let speed = if elapsed.as_secs_f64() > 0.0 {
        (total as f64 / elapsed.as_secs_f64()) as u64
    } else {
        0
    };
    let elapsed_str = format_duration(elapsed);

    eprintln!("[done]");
    eprintln!("  total tx:            {}", format_number(total));
    eprintln!("  matched tx:          {}", format_number(matched));
    eprintln!("  metadata errors:     {}", format_number(metadata_errors));
    eprintln!("  blocks:              {}", format_number(blocks));
    if let Some(slot) = first_slot {
        eprintln!("  first slot:          {}", slot);
    }
    if let Some(slot) = last_slot {
        eprintln!("  last slot:           {}", slot);
    }
    eprintln!("  elapsed:             {}", elapsed_str);
    eprintln!("  avg speed:           {} tx/s", format_number(speed));
}

fn format_duration(duration: std::time::Duration) -> String {
    let secs = duration.as_secs();
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

fn format_number(value: u64) -> String {
    let s = value.to_string();
    let mut result = String::new();
    let mut count = 0;
    for ch in s.chars().rev() {
        if count != 0 && count % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
        count += 1;
    }
    result.chars().rev().collect()
}

fn build_schema() -> SchemaRef {
    let account_balance_struct = DataType::Struct(Fields::from(vec![
        Field::new("account", DataType::Utf8, false),
        Field::new("pre_lamports", DataType::UInt64, false),
        Field::new("post_lamports", DataType::UInt64, false),
        Field::new("delta_lamports", DataType::Int64, false),
    ]));
    let token_balance_struct = DataType::Struct(Fields::from(vec![
        Field::new("mint", DataType::Utf8, false),
        Field::new("owner", DataType::Utf8, false),
        Field::new("account_index", DataType::UInt32, false),
        Field::new("pre_ui_amount", DataType::Utf8, true),
        Field::new("post_ui_amount", DataType::Utf8, true),
    ]));
    let instruction_struct = DataType::Struct(Fields::from(vec![
        Field::new("program_id", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, false),
        Field::new(
            "accounts",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
    ]));

    Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("block_time", DataType::Int64, true),
        Field::new(
            "signatures",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new(
            "accounts",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new(
            "program_ids",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new("fee", DataType::UInt64, false),
        Field::new(
            "account_balance_deltas",
            DataType::List(Arc::new(Field::new("item", account_balance_struct, true))),
            false,
        ),
        Field::new(
            "token_balance_deltas",
            DataType::List(Arc::new(Field::new("item", token_balance_struct, true))),
            false,
        ),
        Field::new(
            "log_messages",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("err", DataType::Utf8, true),
        Field::new(
            "instructions",
            DataType::List(Arc::new(Field::new("item", instruction_struct, true))),
            false,
        ),
    ]))
}

fn flush_batch_sync(
    schema: &SchemaRef,
    writer: &mut ArrowWriter<std::fs::File>,
    records: Vec<WhalesV1Record>,
) -> anyhow::Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    let rows = records.len();

    let mut slot_builder = UInt64Builder::with_capacity(rows);
    let mut block_time_builder = Int64Builder::with_capacity(rows);
    let mut signatures_builder = ListBuilder::with_capacity(StringBuilder::new(), rows);
    let mut accounts_builder = ListBuilder::with_capacity(StringBuilder::new(), rows);
    let mut program_ids_builder = ListBuilder::with_capacity(StringBuilder::new(), rows);
    let mut fee_builder = UInt64Builder::with_capacity(rows);

    let account_balance_struct_builder = StructBuilder::new(
        vec![
            Field::new("account", DataType::Utf8, false),
            Field::new("pre_lamports", DataType::UInt64, false),
            Field::new("post_lamports", DataType::UInt64, false),
            Field::new("delta_lamports", DataType::Int64, false),
        ],
        vec![
            Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
            Box::new(UInt64Builder::new()),
            Box::new(UInt64Builder::new()),
            Box::new(Int64Builder::new()),
        ],
    );
    let mut account_balance_builder =
        ListBuilder::with_capacity(account_balance_struct_builder, rows);

    let token_balance_struct_builder = StructBuilder::new(
        vec![
            Field::new("mint", DataType::Utf8, false),
            Field::new("owner", DataType::Utf8, false),
            Field::new("account_index", DataType::UInt32, false),
            Field::new("pre_ui_amount", DataType::Utf8, true),
            Field::new("post_ui_amount", DataType::Utf8, true),
        ],
        vec![
            Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
            Box::new(StringBuilder::new()),
            Box::new(UInt32Builder::new()),
            Box::new(StringBuilder::new()),
            Box::new(StringBuilder::new()),
        ],
    );
    let mut token_balance_builder = ListBuilder::with_capacity(token_balance_struct_builder, rows);

    let instruction_struct_builder = StructBuilder::new(
        vec![
            Field::new("program_id", DataType::Utf8, false),
            Field::new("data", DataType::Utf8, false),
            Field::new(
                "accounts",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
        ],
        vec![
            Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
            Box::new(StringBuilder::new()),
            Box::new(ListBuilder::new(StringBuilder::new())),
        ],
    );
    let mut instructions_builder = ListBuilder::with_capacity(instruction_struct_builder, rows);

    let mut log_messages_builder = ListBuilder::with_capacity(StringBuilder::new(), rows);
    let mut err_builder = StringBuilder::with_capacity(rows, rows.saturating_mul(16));

    for record in records.iter() {
        slot_builder.append_value(record.slot);
        match record.block_time {
            Some(v) => block_time_builder.append_value(v),
            None => block_time_builder.append_null(),
        }

        let sigs_values = signatures_builder.values();
        for sig in &record.signatures {
            sigs_values.append_value(sig);
        }
        signatures_builder.append(true);

        let accounts_values = accounts_builder.values();
        for account in &record.accounts {
            accounts_values.append_value(account);
        }
        accounts_builder.append(true);

        let program_ids_values = program_ids_builder.values();
        for program_id in &record.program_ids {
            program_ids_values.append_value(program_id);
        }
        program_ids_builder.append(true);

        fee_builder.append_value(record.fee);

        let ab_values = account_balance_builder.values();
        for delta in &record.account_balance_deltas {
            ab_values
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(&delta.account);
            ab_values
                .field_builder::<UInt64Builder>(1)
                .unwrap()
                .append_value(delta.pre_lamports);
            ab_values
                .field_builder::<UInt64Builder>(2)
                .unwrap()
                .append_value(delta.post_lamports);
            ab_values
                .field_builder::<Int64Builder>(3)
                .unwrap()
                .append_value(delta.delta_lamports);
            ab_values.append(true);
        }
        account_balance_builder.append(true);

        let tb_values = token_balance_builder.values();
        for delta in &record.token_balance_deltas {
            tb_values
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(&delta.mint);
            tb_values
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(&delta.owner);
            tb_values
                .field_builder::<UInt32Builder>(2)
                .unwrap()
                .append_value(delta.account_index);
            match &delta.pre_ui_amount {
                Some(v) => tb_values
                    .field_builder::<StringBuilder>(3)
                    .unwrap()
                    .append_value(v),
                None => tb_values
                    .field_builder::<StringBuilder>(3)
                    .unwrap()
                    .append_null(),
            }
            match &delta.post_ui_amount {
                Some(v) => tb_values
                    .field_builder::<StringBuilder>(4)
                    .unwrap()
                    .append_value(v),
                None => tb_values
                    .field_builder::<StringBuilder>(4)
                    .unwrap()
                    .append_null(),
            }
            tb_values.append(true);
        }
        token_balance_builder.append(true);

        match &record.log_messages {
            Some(logs) => {
                let logs_values = log_messages_builder.values();
                for log in logs {
                    logs_values.append_value(log);
                }
                log_messages_builder.append(true);
            }
            None => {
                log_messages_builder.append(false);
            }
        }

        match &record.err {
            Some(err) => err_builder.append_value(err),
            None => err_builder.append_null(),
        }

        let inst_values = instructions_builder.values();
        for inst in &record.instructions {
            inst_values
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(&inst.program_id);
            inst_values
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(&inst.data);
            let accounts_builder = inst_values
                .field_builder::<ListBuilder<StringBuilder>>(2)
                .unwrap();
            let accounts_values = accounts_builder.values();
            for account in &inst.accounts {
                accounts_values.append_value(account);
            }
            accounts_builder.append(true);
            inst_values.append(true);
        }
        instructions_builder.append(true);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(slot_builder.finish()),
        Arc::new(block_time_builder.finish()),
        Arc::new(signatures_builder.finish()),
        Arc::new(accounts_builder.finish()),
        Arc::new(program_ids_builder.finish()),
        Arc::new(fee_builder.finish()),
        Arc::new(account_balance_builder.finish()),
        Arc::new(token_balance_builder.finish()),
        Arc::new(log_messages_builder.finish()),
        Arc::new(err_builder.finish()),
        Arc::new(instructions_builder.finish()),
    ];

    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .context("failed to build record batch for parquet")?;
    writer
        .write(&batch)
        .context("failed to write parquet batch")?;

    Ok(())
}
