//! `harpoon ingest` — full pipeline: download → parse → filter → [decode] → write.

use {
    crate::{
        download,
        pipeline::{self, ExtractMode},
        stats::{PipelineStats, TimingStats},
    },
    anyhow::{anyhow, Context},
    futures_util::TryStreamExt,
    harpoon_decode::IdlDecoder,
    harpoon_export::{FlatPartitionedWriter, OutputFormat, create_writer},
    solana_sdk::pubkey::Pubkey,
    std::{
        collections::HashSet,
        path::Path,
        str::FromStr,
        sync::Arc,
        time::Duration,
    },
};

#[allow(clippy::too_many_arguments)]
pub async fn run(
    epochs: &[u64],
    program_str: &str,
    idl_path: Option<&Path>,
    output_dir: &Path,
    format: OutputFormat,
    stream_download: bool,
    keep_car: bool,
    batch_size: usize,
    extract_mode: ExtractMode,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output dir: {output_dir:?}"))?;

    // Parse target programs
    let target_programs: HashSet<Pubkey> = program_str
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(Pubkey::from_str)
        .collect::<Result<HashSet<_>, _>>()
        .context("failed to parse program IDs")?;

    if target_programs.is_empty() {
        return Err(anyhow!("no program IDs provided"));
    }

    // Load IDL decoder if provided
    let idl_decoder = idl_path
        .map(|p| IdlDecoder::from_idl_path(p).with_context(|| format!("failed to load IDL: {p:?}")))
        .transpose()?
        .map(Arc::new);

    // Build extract schemas from IDL (for events/instructions modes)
    let extract_schemas = if extract_mode != ExtractMode::Raw {
        let idl_json = std::fs::read_to_string(idl_path.unwrap())
            .with_context(|| format!("failed to read IDL: {:?}", idl_path.unwrap()))?;
        let idl: harpoon_decode::Idl = serde_json::from_str(&idl_json)
            .context("failed to parse IDL JSON")?;
        Some(pipeline::build_extract_schemas(&idl, extract_mode))
    } else {
        None
    };

    let target_programs = Arc::new(target_programs);
    let ext = match format {
        OutputFormat::Parquet => "parquet",
        OutputFormat::Csv => "csv",
        OutputFormat::JsonLines => "jsonl",
    };

    eprintln!(
        "Starting ingest for {} epoch(s), {} program(s), format={}, extract={:?}",
        epochs.len(),
        target_programs.len(),
        ext,
        extract_mode,
    );

    for &epoch in epochs {
        let stats = Arc::new(PipelineStats::default());
        let timing = Arc::new(TimingStats::default());

        eprintln!("--- Epoch {epoch} ---");

        match extract_mode {
            ExtractMode::Raw => {
                let output_path = output_dir.join(format!("epoch-{epoch}.{ext}"));
                let writer = create_writer(&output_path, format, batch_size)?;

                run_pipeline_for_epoch(
                    epoch,
                    output_dir,
                    stream_download,
                    keep_car,
                    &target_programs,
                    &idl_decoder,
                    writer,
                    batch_size,
                    stats,
                    timing,
                )
                .await?;
            }
            ExtractMode::Events | ExtractMode::Instructions => {
                let schemas = extract_schemas.as_ref().unwrap().clone();
                let output_subdir = output_dir.join(format!("epoch-{epoch}"));
                let writer = FlatPartitionedWriter::new(&output_subdir, format, schemas)?;
                let decoder = Arc::clone(idl_decoder.as_ref().unwrap());

                run_extract_for_epoch(
                    epoch,
                    output_dir,
                    stream_download,
                    keep_car,
                    &target_programs,
                    decoder,
                    extract_mode,
                    writer,
                    batch_size,
                    stats,
                    timing,
                )
                .await?;
            }
        }
    }

    eprintln!("All epochs complete.");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_pipeline_for_epoch(
    epoch: u64,
    output_dir: &Path,
    stream_download: bool,
    keep_car: bool,
    target_programs: &Arc<HashSet<Pubkey>>,
    idl_decoder: &Option<Arc<IdlDecoder>>,
    writer: Box<dyn harpoon_export::ExportWriter + Send>,
    batch_size: usize,
    stats: Arc<PipelineStats>,
    timing: Arc<TimingStats>,
) -> anyhow::Result<()> {
    if stream_download {
        let url = download::epoch_url(epoch);
        eprintln!("Streaming from {url}");

        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(20))
            .timeout(Duration::from_secs(6 * 60 * 60))
            .build()
            .context("http client")?;
        let resp = client.get(&url).send().await.context("http request")?;
        if !resp.status().is_success() {
            return Err(anyhow!("HTTP {} for {url}", resp.status()));
        }

        let stream = resp.bytes_stream().map_err(std::io::Error::other);
        let reader = tokio_util::io::StreamReader::new(stream);

        pipeline::run_async_pipeline(
            Box::new(reader),
            Arc::clone(target_programs),
            idl_decoder.clone(),
            writer,
            batch_size,
            stats,
            timing,
        )
        .await
    } else {
        let car_path = download::download_car(epoch, output_dir)?;

        let file = tokio::fs::File::open(&car_path).await?;
        let reader = tokio::io::BufReader::new(file);
        pipeline::run_async_pipeline(
            Box::new(reader),
            Arc::clone(target_programs),
            idl_decoder.clone(),
            writer,
            batch_size,
            stats,
            timing,
        )
        .await?;

        if !keep_car {
            download::remove_car(&car_path)?;
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_extract_for_epoch(
    epoch: u64,
    output_dir: &Path,
    stream_download: bool,
    keep_car: bool,
    target_programs: &Arc<HashSet<Pubkey>>,
    idl_decoder: Arc<IdlDecoder>,
    extract_mode: ExtractMode,
    writer: FlatPartitionedWriter,
    batch_size: usize,
    stats: Arc<PipelineStats>,
    timing: Arc<TimingStats>,
) -> anyhow::Result<()> {
    if stream_download {
        let url = download::epoch_url(epoch);
        eprintln!("Streaming from {url}");

        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(20))
            .timeout(Duration::from_secs(6 * 60 * 60))
            .build()
            .context("http client")?;
        let resp = client.get(&url).send().await.context("http request")?;
        if !resp.status().is_success() {
            return Err(anyhow!("HTTP {} for {url}", resp.status()));
        }

        let stream = resp.bytes_stream().map_err(std::io::Error::other);
        let reader = tokio_util::io::StreamReader::new(stream);

        pipeline::run_extract_pipeline(
            Box::new(reader),
            Arc::clone(target_programs),
            idl_decoder,
            extract_mode,
            writer,
            batch_size,
            stats,
            timing,
        )
        .await
    } else {
        let car_path = download::download_car(epoch, output_dir)?;

        let file = tokio::fs::File::open(&car_path).await?;
        let reader = tokio::io::BufReader::new(file);
        pipeline::run_extract_pipeline(
            Box::new(reader),
            Arc::clone(target_programs),
            idl_decoder,
            extract_mode,
            writer,
            batch_size,
            stats,
            timing,
        )
        .await?;

        if !keep_car {
            download::remove_car(&car_path)?;
        }
        Ok(())
    }
}
