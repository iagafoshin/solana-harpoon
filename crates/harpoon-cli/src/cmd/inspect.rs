//! `harpoon inspect` — print stats about a CAR file or epoch.

use {
    anyhow::{anyhow, Context},
    harpoon_car::{
        MmapNodeReader,
        node::{Kind, Node},
    },
    indicatif::{ProgressBar, ProgressStyle},
    std::{collections::HashMap, path::Path},
};

pub async fn run(car_path: Option<&Path>, epoch: Option<u64>) -> anyhow::Result<()> {
    let path = match (car_path, epoch) {
        (Some(p), _) => p.to_path_buf(),
        (None, Some(epoch)) => {
            // Download to temp dir
            let tmp = std::env::temp_dir().join("harpoon-inspect");
            std::fs::create_dir_all(&tmp)?;
            crate::download::download_car(epoch, &tmp)?
        }
        (None, None) => {
            return Err(anyhow!("provide either --car <path> or --epoch <N>"));
        }
    };

    eprintln!("Inspecting {path:?}");

    let reader = MmapNodeReader::open(&path)
        .with_context(|| format!("failed to open {path:?}"))?;

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {msg}")
            .expect("valid template"),
    );

    let mut counts: HashMap<Kind, u64> = HashMap::new();
    let mut total_nodes = 0u64;
    let mut first_slot: Option<u64> = None;
    let mut last_slot: Option<u64> = None;
    let program_counts: HashMap<String, u64> = HashMap::new();
    let mut tx_count = 0u64;

    for raw_result in reader {
        let raw = match raw_result {
            Ok(r) => r,
            Err(err) => {
                eprintln!("[warn] node read error: {err}");
                continue;
            }
        };

        total_nodes += 1;
        if total_nodes % 50_000 == 0 {
            pb.set_message(format!("nodes={total_nodes} tx={tx_count}"));
        }

        let node = match Node::try_from(raw.get_data()) {
            Ok(n) => n,
            Err(_) => continue,
        };

        *counts.entry(node.kind()).or_default() += 1;

        match &node {
            Node::Transaction(frame) => {
                tx_count += 1;
                if first_slot.is_none() {
                    first_slot = Some(frame.slot);
                }
                last_slot = Some(frame.slot);
            }
            Node::Block(block) => {
                if first_slot.is_none() {
                    first_slot = Some(block.slot);
                }
                last_slot = Some(block.slot);
            }
            _ => {}
        }
    }

    pb.finish_and_clear();

    // Print results
    eprintln!("\n=== CAR Inspection ===");
    eprintln!("File:          {path:?}");
    eprintln!("Total nodes:   {total_nodes}");
    eprintln!();
    eprintln!("Node counts by kind:");
    let mut sorted_counts: Vec<_> = counts.into_iter().collect();
    sorted_counts.sort_by_key(|(k, _)| k.to_u64());
    for (kind, count) in &sorted_counts {
        eprintln!("  {kind:<14?} {count}");
    }
    eprintln!();
    if let Some(first) = first_slot {
        eprintln!("First slot:    {first}");
    }
    if let Some(last) = last_slot {
        eprintln!("Last slot:     {last}");
    }
    eprintln!("Transactions:  {tx_count}");

    if !program_counts.is_empty() {
        eprintln!();
        eprintln!("Top programs:");
        let mut sorted: Vec<_> = program_counts.into_iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1));
        for (program, count) in sorted.iter().take(20) {
            eprintln!("  {program}  {count}");
        }
    }

    Ok(())
}
