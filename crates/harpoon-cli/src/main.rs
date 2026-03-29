use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod cmd;
mod download;
mod pipeline;
mod stats;

use {
    clap::{Parser, Subcommand},
    std::path::PathBuf,
};

#[derive(Debug, Parser)]
#[command(name = "harpoon", about = "Solana CAR archive toolkit", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Full pipeline: download CAR → parse → filter → [decode] → write output.
    Ingest {
        /// Epoch range, e.g. "880..895" or single epoch "880".
        #[arg(long)]
        epochs: String,

        /// Target program IDs (comma-separated).
        #[arg(long)]
        program: String,

        /// Path to Anchor IDL JSON for instruction/event decoding.
        #[arg(long)]
        idl: Option<PathBuf>,

        /// Output directory for Parquet/CSV/JSONL files.
        #[arg(long, default_value = "./data")]
        output: PathBuf,

        /// Output format: parquet, csv, jsonl.
        #[arg(long, default_value = "parquet")]
        format: String,

        /// Stream-download CAR files over HTTP without saving to disk.
        #[arg(long)]
        stream_download: bool,

        /// Keep downloaded CAR files after processing.
        #[arg(long)]
        keep_car: bool,

        /// Batch size for Parquet writer (records per batch).
        #[arg(long, default_value_t = 50_000)]
        batch_size: usize,

        /// Extraction mode: raw (full transactions), events (decoded events from logs),
        /// instructions (decoded instructions). Requires --idl for events/instructions.
        #[arg(long, default_value = "raw")]
        extract: String,
    },

    /// Offline decode: apply IDL decoder to existing Parquet with raw instruction data.
    Decode {
        /// Path to Anchor IDL JSON.
        #[arg(long)]
        idl: PathBuf,

        /// Input directory with raw Parquet files.
        #[arg(long)]
        input: PathBuf,

        /// Output directory for decoded files.
        #[arg(long)]
        output: PathBuf,

        /// Output format: parquet, csv, jsonl.
        #[arg(long, default_value = "parquet")]
        format: String,

        /// What to extract: events, instructions, or both (default).
        #[arg(long, default_value = "both")]
        extract: String,
    },

    /// Print statistics about a CAR file or epoch.
    Inspect {
        /// Path to local CAR file.
        #[arg(long, conflicts_with = "epoch")]
        car: Option<PathBuf>,

        /// Epoch number to download and inspect.
        #[arg(long, conflicts_with = "car")]
        epoch: Option<u64>,
    },
}

fn parse_extract_mode(s: &str) -> anyhow::Result<pipeline::ExtractMode> {
    match s {
        "raw" => Ok(pipeline::ExtractMode::Raw),
        "events" => Ok(pipeline::ExtractMode::Events),
        "instructions" => Ok(pipeline::ExtractMode::Instructions),
        other => anyhow::bail!("unknown extract mode: {other} (expected: raw, events, instructions)"),
    }
}

fn parse_decode_mode(s: &str) -> anyhow::Result<cmd::decode::DecodeMode> {
    match s {
        "events" => Ok(cmd::decode::DecodeMode::Events),
        "instructions" => Ok(cmd::decode::DecodeMode::Instructions),
        "both" => Ok(cmd::decode::DecodeMode::Both),
        other => anyhow::bail!("unknown decode extract mode: {other} (expected: events, instructions, both)"),
    }
}

fn parse_output_format(s: &str) -> anyhow::Result<harpoon_export::OutputFormat> {
    match s {
        "parquet" => Ok(harpoon_export::OutputFormat::Parquet),
        "csv" => Ok(harpoon_export::OutputFormat::Csv),
        "jsonl" | "jsonlines" | "json" => Ok(harpoon_export::OutputFormat::JsonLines),
        other => anyhow::bail!("unknown output format: {other} (expected: parquet, csv, jsonl)"),
    }
}

fn parse_epoch_range(s: &str) -> anyhow::Result<Vec<u64>> {
    if let Some((start, end)) = s.split_once("..") {
        let start: u64 = start.trim().parse()?;
        let end: u64 = end.trim().parse()?;
        if start > end {
            anyhow::bail!("invalid epoch range: {start}..{end}");
        }
        Ok((start..=end).collect())
    } else {
        let epoch: u64 = s.trim().parse()?;
        Ok(vec![epoch])
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Ingest {
            epochs,
            program,
            idl,
            output,
            format,
            stream_download,
            keep_car,
            batch_size,
            extract,
        } => {
            let extract_mode = parse_extract_mode(&extract)?;
            if extract_mode != pipeline::ExtractMode::Raw && idl.is_none() {
                anyhow::bail!("--idl is required for --extract events/instructions");
            }
            cmd::ingest::run(
                &parse_epoch_range(&epochs)?,
                &program,
                idl.as_deref(),
                &output,
                parse_output_format(&format)?,
                stream_download,
                keep_car,
                batch_size,
                extract_mode,
            )
            .await
        }
        Commands::Decode {
            idl,
            input,
            output,
            format,
            extract,
        } => {
            let mode = parse_decode_mode(&extract)?;
            cmd::decode::run(&idl, &input, &output, parse_output_format(&format)?, mode).await
        }
        Commands::Inspect { car, epoch } => cmd::inspect::run(car.as_deref(), epoch).await,
    }
}
