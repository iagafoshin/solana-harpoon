use {
    aho_corasick::AhoCorasick,
    anyhow::{Context, anyhow},
    arrow_array::{
        ArrayRef, RecordBatch,
        builder::{
            ArrayBuilder, Int64Builder, ListBuilder, StringBuilder, StructBuilder, UInt32Builder,
            UInt64Builder,
        },
    },
    arrow_schema::{DataType, Field, Fields, Schema, SchemaRef},
    bs58,
    clap::Parser,
    indicatif::{MultiProgress, ProgressBar, ProgressStyle},
    parquet::arrow::ArrowWriter,
    prost::Message,
    rayon::prelude::*,
    serde::{Deserialize, Serialize},
    solana_sdk::{
        instruction::CompiledInstruction, message::VersionedMessage, pubkey::Pubkey,
        transaction::VersionedTransaction,
    },
    solana_storage_proto::{StoredTransactionStatusMeta, convert::generated},
    solana_transaction_status::{TransactionStatusMeta, TransactionTokenBalance},
    std::{
        collections::{BTreeMap, BTreeSet, HashSet},
        convert::TryFrom,
        path::PathBuf,
        process::Command,
        str::FromStr,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, Instant},
    },
    tokio::{fs, fs::File, io::BufReader},
    yellowstone_faithful_car_parser::node::{Node, NodeReader, Nodes},
};

const DEFAULT_PROGRAMS: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PROGRESS_INTERVAL: u64 = 10_000;
const BATCH_SIZE: usize = 5_000;

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

    /// Maximum number of epochs processed concurrently
    #[clap(short = 'j', long, default_value_t = 4)]
    pub concurrency: usize,

    /// Parse Nodes from CAR file
    #[clap(long)]
    pub parse: bool,

    /// Decode Nodes to Solana structs
    #[clap(long)]
    pub decode: bool,

    /// Target program ids (comma-separated)
    #[clap(long)]
    pub program: Option<String>,
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
    let target_program_bytes: Vec<Vec<u8>> = target_programs
        .iter()
        .map(|p| p.to_bytes().to_vec())
        .collect();
    let searcher = Arc::new(
        AhoCorasick::new(target_program_bytes).context("failed to build aho-corasick searcher")?,
    );
    let target_programs = Arc::new(target_programs);
    eprintln!(
        "Starting pipeline for programs [{}] across {} epochs (latest={}), concurrency={}",
        program_list, args.num_epochs, args.latest_epoch, args.concurrency
    );

    let semaphore = Arc::new(tokio::sync::Semaphore::new(args.concurrency));
    let mut handles = Vec::new();

    for offset in 0..args.num_epochs {
        let epoch = args.latest_epoch.saturating_sub(offset);
        let args_clone = args.clone();
        let sem = semaphore.clone();
        let permit = sem.acquire_owned().await?;
        let target_programs = target_programs.clone();
        let searcher = searcher.clone();

        let handle = tokio::spawn(async move {
            let _permit = permit;
            let car_filename = format!("epoch-{epoch}.car");
            let parquet_filename = format!("whales-epoch-{epoch}.parquet");
            let car_path = args_clone.data_dir.join(&car_filename);
            let output_path = args_clone.data_dir.join(&parquet_filename);

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
                    .arg("-o")
                    .arg(&car_filename)
                    .arg(&url)
                    .current_dir(&args_clone.data_dir)
                    .status()
                    .context("failed to spawn aria2c")?;
                if !status.success() {
                    return Err(anyhow!("aria2c failed for epoch {epoch}"));
                }
            }

            process_epoch_file(
                car_path.clone(),
                output_path,
                &args_clone,
                target_programs,
                searcher,
            )
            .await?;
            fs::remove_file(&car_path)
                .await
                .context("failed to remove downloaded CAR")?;

            Ok::<_, anyhow::Error>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle
            .await
            .context("task join error")?
            .context("task failed")?;
    }

    Ok(())
}

async fn process_epoch_file(
    car_path: PathBuf,
    output_path: PathBuf,
    args: &Args,
    target_programs: Arc<Vec<Pubkey>>,
    searcher: Arc<AhoCorasick>,
) -> anyhow::Result<()> {
    let target_program_set: HashSet<Pubkey> = target_programs.iter().cloned().collect();
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
    let mut reader = NodeReader::new(BufReader::new(file));

    if !args.parse {
        let bar = ProgressBar::no_length()
            .with_style(ProgressStyle::with_template("{spinner} {pos}").expect("valid template"));
        let mut counter = 0;
        while reader.read_node().await?.is_some() {
            counter += 1;
            if counter >= 131_072 {
                bar.inc(counter);
                counter = 0;
            }
        }
        bar.inc(counter);
        bar.finish();
        return Ok(());
    }

    let output_file = std::fs::File::create(&output_path)
        .with_context(|| format!("failed to create {:?}", output_path))?;
    let schema = build_schema();
    let mut writer = ArrowWriter::try_new(output_file, schema.clone(), None)
        .context("failed to create parquet writer")?;
    let mut buffer: Vec<WhalesV1Record> = Vec::new();

    let start_time = Instant::now();
    let stats = Stats::default();
    let mut first_slot = None;
    let mut last_slot = None;
    let progress_ui = StatsProgress::new();
    progress_ui.update(&stats, last_slot, start_time);

    loop {
        #[cfg(feature = "timing")]
        let read_start = Instant::now();
        let nodes = Nodes::read_until_block(&mut reader).await?;
        #[cfg(feature = "timing")]
        timing_add(TimingPhase::ReadNodes, read_start.elapsed());
        if nodes.nodes.is_empty() {
            break;
        }

        let block_time = nodes.nodes.values().find_map(|node| match node {
            Node::Block(block) => i64::try_from(block.meta.blocktime).ok(),
            _ => None,
        });
        #[cfg(feature = "timing")]
        let prepare_start = Instant::now();
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
                        progress_ui.update(&stats, last_slot, start_time);
                    }

                    if !args.decode {
                        continue;
                    }

                    if searcher.find(frame.data.data.as_slice()).is_none() {
                        continue;
                    }

                    let metadata_frames = nodes
                        .reassemble_dataframes(&frame.metadata)
                        .context("failed to reassemble tx metadata")?;
                    let meta_data = if metadata_frames.is_empty() {
                        None
                    } else {
                        Some(metadata_frames)
                    };
                    prepared_txs.push(PreparedTx {
                        slot: frame.slot,
                        block_time,
                        tx_data: frame.data.data.clone(),
                        meta_data,
                    });
                }
                Node::Rewards(frame) => {
                    if !args.decode {
                        continue;
                    }

                    let buffer = nodes
                        .reassemble_dataframes(&frame.data)
                        .context("failed to reassemble rewards")?;
                    let buffer = zstd::decode_all(buffer.as_slice())
                        .context("failed to decompress rewards")?;
                    let _ = decode_protobuf_bincode::<Vec<StoredBlockReward>, generated::Rewards>(
                        "rewards", &buffer,
                    );
                }
                Node::Block(_) => {
                    stats.total_blocks.fetch_add(1, Ordering::Relaxed);
                }
                Node::Entry(_) | Node::Subset(_) | Node::Epoch(_) | Node::DataFrame(_) => (),
            }
        }

        #[cfg(feature = "timing")]
        timing_add(TimingPhase::PrepareTxs, prepare_start.elapsed());

        if args.decode && !prepared_txs.is_empty() {
            let records = process_prepared_transactions(prepared_txs, &target_program_set, &stats);
            buffer.extend(records);
            if buffer.len() >= BATCH_SIZE {
                flush_batch(&schema, &mut writer, &mut buffer)?;
            }
        }
    }

    flush_batch(&schema, &mut writer, &mut buffer)?;
    writer.close().context("failed to finalize parquet file")?;

    let elapsed = start_time.elapsed();
    progress_ui.finish();
    eprintln!();
    print_final_summary(&stats, first_slot, last_slot, elapsed);
    print_timing_summary(elapsed);

    Ok(())
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

#[allow(dead_code)]
#[derive(Deserialize)]
struct StoredBlockReward {
    pubkey: String,
    lamports: i64,
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

struct Stats {
    total_transactions: AtomicU64,
    matching_transactions: AtomicU64,
    total_blocks: AtomicU64,
    skipped_metadata_errors: AtomicU64,
    skipped_no_program: AtomicU64,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            total_transactions: AtomicU64::new(0),
            matching_transactions: AtomicU64::new(0),
            total_blocks: AtomicU64::new(0),
            skipped_metadata_errors: AtomicU64::new(0),
            skipped_no_program: AtomicU64::new(0),
        }
    }
}

struct PreparedTx {
    slot: u64,
    block_time: Option<i64>,
    tx_data: Vec<u8>,
    meta_data: Option<Vec<u8>>,
}

struct StatsProgress {
    _multi: MultiProgress,
    pb_total: ProgressBar,
    pb_matched: ProgressBar,
    pb_skipped_meta: ProgressBar,
    pb_skipped_no_prog: ProgressBar,
    pb_blocks: ProgressBar,
    pb_slot: ProgressBar,
    pb_speed: ProgressBar,
    pb_elapsed: ProgressBar,
}

impl StatsProgress {
    fn new() -> Self {
        let multi = MultiProgress::new();
        let pb_total = Self::make_bar(&multi);
        let pb_matched = Self::make_bar(&multi);
        let pb_skipped_meta = Self::make_bar(&multi);
        let pb_skipped_no_prog = Self::make_bar(&multi);
        let pb_blocks = Self::make_bar(&multi);
        let pb_slot = Self::make_bar(&multi);
        let pb_speed = Self::make_bar(&multi);
        let pb_elapsed = Self::make_bar(&multi);
        Self {
            _multi: multi,
            pb_total,
            pb_matched,
            pb_skipped_meta,
            pb_skipped_no_prog,
            pb_blocks,
            pb_slot,
            pb_speed,
            pb_elapsed,
        }
    }

    fn make_bar(multi: &MultiProgress) -> ProgressBar {
        let pb = multi.add(ProgressBar::new_spinner());
        pb.set_style(
            ProgressStyle::with_template("{msg}").expect("progress template must be valid"),
        );
        pb.enable_steady_tick(Duration::from_millis(250));
        pb
    }

    fn update(&self, stats: &Stats, last_slot: Option<u64>, start_time: Instant) {
        let total = stats.total_transactions.load(Ordering::Relaxed);
        let matched = stats.matching_transactions.load(Ordering::Relaxed);
        let skipped_meta = stats.skipped_metadata_errors.load(Ordering::Relaxed);
        let skipped_no_prog = stats.skipped_no_program.load(Ordering::Relaxed);
        let blocks = stats.total_blocks.load(Ordering::Relaxed);
        let slot = last_slot.unwrap_or(0);
        let elapsed = start_time.elapsed();
        let elapsed_str = format_duration(elapsed);
        let speed = if elapsed.as_secs_f64() > 0.0 {
            (total as f64 / elapsed.as_secs_f64()) as u64
        } else {
            0
        };

        self.pb_total
            .set_message(format!("total tx:         {}", format_number(total)));
        self.pb_matched
            .set_message(format!("matched tx:       {}", format_number(matched)));
        self.pb_skipped_meta
            .set_message(format!("skipped bad_meta: {}", format_number(skipped_meta)));
        self.pb_skipped_no_prog.set_message(format!(
            "skipped no_prog:  {}",
            format_number(skipped_no_prog)
        ));
        self.pb_blocks
            .set_message(format!("blocks:           {}", format_number(blocks)));
        self.pb_slot
            .set_message(format!("slot:             {}", format_number(slot)));
        self.pb_speed
            .set_message(format!("speed:            {} tx/s", format_number(speed)));
        self.pb_elapsed
            .set_message(format!("elapsed:          {}", elapsed_str));
    }

    fn finish(&self) {
        self.pb_total.finish_and_clear();
        self.pb_matched.finish_and_clear();
        self.pb_skipped_meta.finish_and_clear();
        self.pb_skipped_no_prog.finish_and_clear();
        self.pb_blocks.finish_and_clear();
        self.pb_slot.finish_and_clear();
        self.pb_speed.finish_and_clear();
        self.pb_elapsed.finish_and_clear();
    }
}

#[allow(dead_code)]
enum TimingPhase {
    ReadNodes,
    PrepareTxs,
    TxDecode,
    MetaDecompress,
    MetaDecode,
    ExtendAccountKeys,
    HasProgramCheck,
    BuildDeltas,
    JsonSerialize,
}

#[cfg(feature = "timing")]
struct TimingStats {
    read_nodes: AtomicU64,
    prepare_txs: AtomicU64,
    tx_decode: AtomicU64,
    meta_decompress: AtomicU64,
    meta_decode: AtomicU64,
    extend_account_keys: AtomicU64,
    has_program_check: AtomicU64,
    build_deltas: AtomicU64,
    json_serialize: AtomicU64,
}

#[cfg(feature = "timing")]
impl TimingStats {
    const fn new() -> Self {
        Self {
            read_nodes: AtomicU64::new(0),
            prepare_txs: AtomicU64::new(0),
            tx_decode: AtomicU64::new(0),
            meta_decompress: AtomicU64::new(0),
            meta_decode: AtomicU64::new(0),
            extend_account_keys: AtomicU64::new(0),
            has_program_check: AtomicU64::new(0),
            build_deltas: AtomicU64::new(0),
            json_serialize: AtomicU64::new(0),
        }
    }

    fn add(&self, phase: TimingPhase, duration: Duration) {
        let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
        let atomic = match phase {
            TimingPhase::ReadNodes => &self.read_nodes,
            TimingPhase::PrepareTxs => &self.prepare_txs,
            TimingPhase::TxDecode => &self.tx_decode,
            TimingPhase::MetaDecompress => &self.meta_decompress,
            TimingPhase::MetaDecode => &self.meta_decode,
            TimingPhase::ExtendAccountKeys => &self.extend_account_keys,
            TimingPhase::HasProgramCheck => &self.has_program_check,
            TimingPhase::BuildDeltas => &self.build_deltas,
            TimingPhase::JsonSerialize => &self.json_serialize,
        };
        atomic.fetch_add(nanos, Ordering::Relaxed);
    }

    fn get(&self, phase: TimingPhase) -> Duration {
        let nanos = match phase {
            TimingPhase::ReadNodes => self.read_nodes.load(Ordering::Relaxed),
            TimingPhase::PrepareTxs => self.prepare_txs.load(Ordering::Relaxed),
            TimingPhase::TxDecode => self.tx_decode.load(Ordering::Relaxed),
            TimingPhase::MetaDecompress => self.meta_decompress.load(Ordering::Relaxed),
            TimingPhase::MetaDecode => self.meta_decode.load(Ordering::Relaxed),
            TimingPhase::ExtendAccountKeys => self.extend_account_keys.load(Ordering::Relaxed),
            TimingPhase::HasProgramCheck => self.has_program_check.load(Ordering::Relaxed),
            TimingPhase::BuildDeltas => self.build_deltas.load(Ordering::Relaxed),
            TimingPhase::JsonSerialize => self.json_serialize.load(Ordering::Relaxed),
        };
        Duration::from_nanos(nanos)
    }
}

#[cfg(feature = "timing")]
static TIMING: TimingStats = TimingStats::new();

#[cfg(feature = "timing")]
fn timing_add(phase: TimingPhase, duration: Duration) {
    TIMING.add(phase, duration);
}

#[cfg(feature = "timing")]
fn print_timing_summary(total_elapsed: Duration) {
    fn percent(part: Duration, total: Duration) -> f64 {
        if total.as_nanos() == 0 {
            0.0
        } else {
            part.as_secs_f64() / total.as_secs_f64() * 100.0
        }
    }

    let phases = [
        (TimingPhase::ReadNodes, "phase_A_read_nodes"),
        (TimingPhase::PrepareTxs, "phase_B_prepare_txs"),
        (TimingPhase::TxDecode, "phase_C_tx_decode"),
        (TimingPhase::MetaDecompress, "phase_C_meta_decompress"),
        (TimingPhase::MetaDecode, "phase_C_meta_decode"),
        (
            TimingPhase::ExtendAccountKeys,
            "phase_C_extend_account_keys",
        ),
        (TimingPhase::HasProgramCheck, "phase_C_has_program_check"),
        (TimingPhase::BuildDeltas, "phase_C_build_deltas"),
        (
            TimingPhase::JsonSerialize,
            "phase_C_json_serialize_and_print",
        ),
    ];

    eprintln!("[timing]");
    eprintln!("  total_elapsed:        {}", format_duration(total_elapsed));
    for (phase, label) in phases {
        let duration = TIMING.get(phase);
        eprintln!(
            "  {label:<24} {:>8.3} s ({:>5.1}%)",
            duration.as_secs_f64(),
            percent(duration, total_elapsed)
        );
    }
}

#[cfg(not(feature = "timing"))]
fn print_timing_summary(_: Duration) {}

fn process_prepared_transactions(
    prepared: Vec<PreparedTx>,
    target_programs: &HashSet<Pubkey>,
    stats: &Stats,
) -> Vec<WhalesV1Record> {
    prepared
        .into_par_iter()
        .filter_map(|tx| {
            let slot = tx.slot;
            match process_prepared_tx(tx, target_programs, stats) {
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
) -> anyhow::Result<Option<WhalesV1Record>> {
    #[cfg(feature = "timing")]
    let tx_decode_start = Instant::now();
    let tx: VersionedTransaction =
        bincode::deserialize(&prepared.tx_data).context("failed to parse tx")?;
    #[cfg(feature = "timing")]
    timing_add(TimingPhase::TxDecode, tx_decode_start.elapsed());

    let (account_keys, instructions) = match &tx.message {
        VersionedMessage::Legacy(msg) => (&msg.account_keys, &msg.instructions),
        VersionedMessage::V0(msg) => (&msg.account_keys, &msg.instructions),
    };

    #[cfg(feature = "timing")]
    let program_check_start = Instant::now();
    let involves_program = account_keys.iter().any(|key| target_programs.contains(key));
    #[cfg(feature = "timing")]
    timing_add(TimingPhase::HasProgramCheck, program_check_start.elapsed());
    if !involves_program {
        stats.skipped_no_program.fetch_add(1, Ordering::Relaxed);
        return Ok(None);
    }

    let mut meta: Option<TransactionStatusMeta> = None;
    if let Some(meta_data) = prepared.meta_data {
        if !meta_data.is_empty() {
            #[cfg(feature = "timing")]
            let meta_decompress_start = Instant::now();
            let buffer = match zstd::decode_all(meta_data.as_slice()) {
                Ok(buffer) => buffer,
                Err(err) => {
                    #[cfg(feature = "timing")]
                    timing_add(TimingPhase::MetaDecompress, meta_decompress_start.elapsed());
                    eprintln!(
                        "[warn] failed to decompress tx metadata at slot {}: {err}",
                        prepared.slot
                    );
                    stats
                        .skipped_metadata_errors
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                }
            };
            #[cfg(feature = "timing")]
            timing_add(TimingPhase::MetaDecompress, meta_decompress_start.elapsed());
            #[cfg(feature = "timing")]
            let meta_decode_start = Instant::now();
            match decode_transaction_status_meta(&buffer) {
                Ok(decoded) => {
                    #[cfg(feature = "timing")]
                    timing_add(TimingPhase::MetaDecode, meta_decode_start.elapsed());
                    meta = Some(decoded);
                }
                Err(err) => {
                    #[cfg(feature = "timing")]
                    timing_add(TimingPhase::MetaDecode, meta_decode_start.elapsed());
                    eprintln!(
                        "[warn] failed to decode tx metadata at slot {}: {err}",
                        prepared.slot
                    );
                    stats
                        .skipped_metadata_errors
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                }
            }
        }
    }

    #[cfg(feature = "timing")]
    let extend_keys_start = Instant::now();
    let mut full_account_keys: Vec<Pubkey> = account_keys.clone();
    if let (VersionedMessage::V0(_), Some(meta)) = (&tx.message, meta.as_ref()) {
        full_account_keys.extend(meta.loaded_addresses.writable.iter().cloned());
        full_account_keys.extend(meta.loaded_addresses.readonly.iter().cloned());
    }
    #[cfg(feature = "timing")]
    timing_add(TimingPhase::ExtendAccountKeys, extend_keys_start.elapsed());

    let inner_instruction_sets = meta
        .as_ref()
        .and_then(|meta| meta.inner_instructions.as_ref());

    let mut program_ids = BTreeSet::new();
    for ix in instructions {
        program_ids.insert(resolve_program_id(&full_account_keys, ix.program_id_index));
    }
    if let Some(inner_sets) = inner_instruction_sets {
        for inner in inner_sets {
            for ix in &inner.instructions {
                program_ids.insert(resolve_program_id(
                    &full_account_keys,
                    ix.instruction.program_id_index,
                ));
            }
        }
    }

    let parsed_instructions =
        build_parsed_instructions(&full_account_keys, instructions, inner_instruction_sets);

    #[cfg(feature = "timing")]
    let build_deltas_start = Instant::now();
    let account_balance_deltas = meta
        .as_ref()
        .map(|meta| build_account_balance_deltas(meta, &full_account_keys))
        .unwrap_or_default();
    let token_balance_deltas = meta
        .as_ref()
        .map(build_token_balance_deltas)
        .unwrap_or_default();
    #[cfg(feature = "timing")]
    timing_add(TimingPhase::BuildDeltas, build_deltas_start.elapsed());
    let fee = meta.as_ref().map(|meta| meta.fee).unwrap_or_default();
    let record = WhalesV1Record {
        slot: prepared.slot,
        block_time: prepared.block_time,
        signatures: tx.signatures.iter().map(|sig| sig.to_string()).collect(),
        accounts: full_account_keys
            .iter()
            .map(|key| key.to_string())
            .collect(),
        program_ids: program_ids.into_iter().collect(),
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
    #[cfg(feature = "timing")]
    let json_start = Instant::now();
    #[cfg(feature = "timing")]
    timing_add(TimingPhase::JsonSerialize, json_start.elapsed());

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

fn resolve_program_id(account_keys: &[Pubkey], program_id_index: u8) -> String {
    account_keys
        .get(program_id_index as usize)
        .map(|key| key.to_string())
        .unwrap_or_else(|| format!("UNKNOWN_PROGRAM_INDEX_{program_id_index}"))
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
    fn apply_balances<'a>(
        map: &mut BTreeMap<(String, String, u8), TokenBalanceDelta>,
        balances: impl Iterator<Item = &'a TransactionTokenBalance>,
        is_post: bool,
    ) {
        for balance in balances {
            let key = (
                balance.mint.clone(),
                balance.owner.clone(),
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
    let skipped_meta = stats.skipped_metadata_errors.load(Ordering::Relaxed);
    let skipped_no_program = stats.skipped_no_program.load(Ordering::Relaxed);
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
    eprintln!("  skipped (bad meta):  {}", format_number(skipped_meta));
    eprintln!(
        "  skipped (no program):{}",
        format_number(skipped_no_program)
    );
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
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
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
            DataType::List(Arc::new(Field::new("item", account_balance_struct, false))),
            false,
        ),
        Field::new(
            "token_balance_deltas",
            DataType::List(Arc::new(Field::new("item", token_balance_struct, false))),
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
            DataType::List(Arc::new(Field::new("item", instruction_struct, false))),
            false,
        ),
    ]))
}

fn flush_batch(
    schema: &SchemaRef,
    writer: &mut ArrowWriter<std::fs::File>,
    buffer: &mut Vec<WhalesV1Record>,
) -> anyhow::Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }

    let mut slot_builder = UInt64Builder::new();
    let mut block_time_builder = Int64Builder::new();
    let mut signatures_builder = ListBuilder::new(StringBuilder::new());
    let mut accounts_builder = ListBuilder::new(StringBuilder::new());
    let mut program_ids_builder = ListBuilder::new(StringBuilder::new());
    let mut fee_builder = UInt64Builder::new();

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
    let mut account_balance_builder = ListBuilder::new(account_balance_struct_builder);

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
    let mut token_balance_builder = ListBuilder::new(token_balance_struct_builder);

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
    let mut instructions_builder = ListBuilder::new(instruction_struct_builder);

    let mut log_messages_builder = ListBuilder::new(StringBuilder::new());
    let mut err_builder = StringBuilder::new();

    for record in buffer.iter() {
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

    buffer.clear();
    Ok(())
}
