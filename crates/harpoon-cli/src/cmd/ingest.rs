//! `harpoon ingest` — full pipeline: download → parse → filter → [decode] → write.

use {
    crate::{
        download,
        pipeline,
        stats::{PipelineStats, TimingStats},
    },
    anyhow::{anyhow, Context},
    futures_util::TryStreamExt,
    harpoon_decode::IdlDecoder,
    harpoon_export::{OutputFormat, create_writer},
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

    let target_programs = Arc::new(target_programs);
    let ext = match format {
        OutputFormat::Parquet => "parquet",
        OutputFormat::Csv => "csv",
        OutputFormat::JsonLines => "jsonl",
    };

    eprintln!(
        "Starting ingest for {} epoch(s), {} program(s), format={}",
        epochs.len(),
        target_programs.len(),
        ext,
    );

    for &epoch in epochs {
        let output_path = output_dir.join(format!("epoch-{epoch}.{ext}"));
        let stats = Arc::new(PipelineStats::default());
        let timing = Arc::new(TimingStats::default());
        let writer = create_writer(&output_path, format, batch_size)?;

        eprintln!("--- Epoch {epoch} ---");

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

            let stream = resp
                .bytes_stream()
                .map_err(std::io::Error::other);
            let reader = tokio_util::io::StreamReader::new(stream);

            pipeline::run_async_pipeline(
                Box::new(reader),
                Arc::clone(&target_programs),
                idl_decoder.clone(),
                writer,
                batch_size,
                stats,
                timing,
            )
            .await?;
        } else {
            let car_path = download::download_car(epoch, output_dir)?;
            eprintln!("Processing {car_path:?} → {output_path:?}");

            let file = tokio::fs::File::open(&car_path).await?;
            let reader = tokio::io::BufReader::new(file);
            pipeline::run_async_pipeline(
                Box::new(reader),
                Arc::clone(&target_programs),
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
        }
    }

    eprintln!("All epochs complete.");
    Ok(())
}
