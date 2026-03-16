//! Pipeline statistics and timing.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

const TIMING_SAMPLE_RATE: u64 = 256;

/// Counters for the ingest pipeline.
pub struct PipelineStats {
    pub total_transactions: AtomicU64,
    pub matching_transactions: AtomicU64,
    pub total_blocks: AtomicU64,
    pub metadata_errors: AtomicU64,
    pub decode_errors: AtomicU64,
    pub metadata_skipped: AtomicU64,
}

impl Default for PipelineStats {
    fn default() -> Self {
        Self {
            total_transactions: AtomicU64::new(0),
            matching_transactions: AtomicU64::new(0),
            total_blocks: AtomicU64::new(0),
            metadata_errors: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            metadata_skipped: AtomicU64::new(0),
        }
    }
}

/// Per-stage timing sampler.
pub struct TimingStats {
    sample_counter: AtomicU64,
    pub tx_decode_ns: AtomicU64,
    pub tx_decode_samples: AtomicU64,
    pub meta_decode_ns: AtomicU64,
    pub meta_decode_samples: AtomicU64,
    pub match_check_ns: AtomicU64,
    pub match_check_samples: AtomicU64,
    pub build_record_ns: AtomicU64,
    pub build_record_samples: AtomicU64,
    pub parquet_write_ns: AtomicU64,
    pub parquet_write_samples: AtomicU64,
}

impl Default for TimingStats {
    fn default() -> Self {
        Self {
            sample_counter: AtomicU64::new(0),
            tx_decode_ns: AtomicU64::new(0),
            tx_decode_samples: AtomicU64::new(0),
            meta_decode_ns: AtomicU64::new(0),
            meta_decode_samples: AtomicU64::new(0),
            match_check_ns: AtomicU64::new(0),
            match_check_samples: AtomicU64::new(0),
            build_record_ns: AtomicU64::new(0),
            build_record_samples: AtomicU64::new(0),
            parquet_write_ns: AtomicU64::new(0),
            parquet_write_samples: AtomicU64::new(0),
        }
    }
}

impl TimingStats {
    pub fn should_sample(&self) -> bool {
        self.sample_counter.fetch_add(1, Ordering::Relaxed) % TIMING_SAMPLE_RATE == 0
    }

    pub fn record(
        &self,
        total_ns: &AtomicU64,
        samples: &AtomicU64,
        duration: Duration,
        do_sample: bool,
    ) {
        if do_sample {
            let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
            total_ns.fetch_add(nanos, Ordering::Relaxed);
            samples.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn print_summary(&self) {
        let stages = [
            ("tx_decode", &self.tx_decode_ns, &self.tx_decode_samples),
            ("meta_decode", &self.meta_decode_ns, &self.meta_decode_samples),
            ("match_check", &self.match_check_ns, &self.match_check_samples),
            (
                "build_record",
                &self.build_record_ns,
                &self.build_record_samples,
            ),
            (
                "parquet_write",
                &self.parquet_write_ns,
                &self.parquet_write_samples,
            ),
        ];
        eprintln!("[timing]");
        for (label, total, count) in stages {
            let c = count.load(Ordering::Relaxed);
            let ns = total.load(Ordering::Relaxed);
            if c == 0 {
                eprintln!(
                    "  {label:<14} total={:>10.3} ms avg={:>8} samples=0",
                    0.0, "n/a"
                );
            } else {
                let avg_ns = ns / c;
                let ms = ns as f64 / 1_000_000.0;
                eprintln!(
                    "  {label:<14} total={ms:>10.3} ms avg={:>8.3} \u{00b5}s samples={c}",
                    avg_ns as f64 / 1_000.0,
                );
            }
        }
    }
}

/// Format a number with thousands separators.
pub fn format_number(value: u64) -> String {
    let s = value.to_string();
    let mut result = String::new();
    for (count, ch) in s.chars().rev().enumerate() {
        if count != 0 && count % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

/// Format a duration as HH:MM:SS.
pub fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}
