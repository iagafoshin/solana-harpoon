//! CAR file download via aria2c or HTTP streaming.

use {
    anyhow::{anyhow, Context},
    std::{
        path::{Path, PathBuf},
        process::Command,
    },
};

/// Base URL for Old Faithful CAR archives.
const BASE_URL: &str = "https://files.old-faithful.net";

/// Build the URL for a given epoch.
pub fn epoch_url(epoch: u64) -> String {
    format!("{BASE_URL}/{epoch}/epoch-{epoch}.car")
}

/// Download a CAR file via aria2c (16 parallel connections, 4MB chunks).
///
/// Downloads to a `.part` temp file first, then renames to final path.
/// Returns `Ok(())` if the file already exists.
pub fn download_car(epoch: u64, data_dir: &Path) -> anyhow::Result<PathBuf> {
    let car_filename = format!("epoch-{epoch}.car");
    let car_path = data_dir.join(&car_filename);

    if car_path.exists() {
        return Ok(car_path);
    }

    let temp_filename = format!("{car_filename}.part");
    let url = epoch_url(epoch);

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
        .current_dir(data_dir)
        .status()
        .context("failed to spawn aria2c — is it installed?")?;

    if !status.success() {
        return Err(anyhow!("aria2c failed for epoch {epoch}"));
    }

    let temp_path = data_dir.join(&temp_filename);
    std::fs::rename(&temp_path, &car_path).with_context(|| {
        format!("failed to rename {temp_path:?} -> {car_path:?}")
    })?;

    Ok(car_path)
}

/// Remove a downloaded CAR file.
pub fn remove_car(path: &Path) -> anyhow::Result<()> {
    std::fs::remove_file(path)
        .with_context(|| format!("failed to remove CAR file: {path:?}"))
}
