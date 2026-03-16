//! Export writers: Parquet (ZSTD), CSV, JSON Lines.
//!
//! All writers implement the [`ExportWriter`] trait for a uniform streaming API.
//! The [`PartitionedWriter`] demultiplexes records by a key (e.g. event name)
//! into separate files.

use {
    crate::{
        record::{FlatRecord, TransactionRecord},
        schema::{build_schema, flat_records_to_batch, records_to_batch},
        Error,
    },
    arrow::datatypes::SchemaRef,
    parquet::{
        arrow::ArrowWriter,
        basic::{Compression, ZstdLevel},
        file::properties::WriterProperties,
    },
    std::{
        collections::HashMap,
        fs::{self, File},
        io::{BufWriter, Write},
        path::{Path, PathBuf},
        sync::Arc,
    },
};

/// Output format for export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Parquet,
    Csv,
    JsonLines,
}

/// Trait for all export writers.
pub trait ExportWriter {
    /// Write a batch of records.
    fn write_records(&mut self, records: &[TransactionRecord]) -> Result<(), Error>;

    /// Flush and finalize the output file.
    fn finish(self: Box<Self>) -> Result<(), Error>;
}

// ---------------------------------------------------------------------------
// Parquet writer
// ---------------------------------------------------------------------------

/// Writes transaction records as Arrow/Parquet with ZSTD compression.
pub struct ParquetExportWriter {
    writer: ArrowWriter<File>,
    schema: SchemaRef,
    batch_size: usize,
    buffer: Vec<TransactionRecord>,
}

impl ParquetExportWriter {
    /// Create a new Parquet writer at `path` with the given batch size.
    pub fn new(path: &Path, batch_size: usize) -> Result<Self, Error> {
        let schema = build_schema();
        let file = File::create(path).map_err(Error::Io)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).expect("valid zstd level")))
            .build();
        let writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))
            .map_err(|e| Error::Parquet(e.to_string()))?;
        Ok(Self {
            writer,
            schema,
            batch_size,
            buffer: Vec::with_capacity(batch_size),
        })
    }

    fn flush_buffer(&mut self) -> Result<(), Error> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let batch = records_to_batch(&self.schema, &self.buffer)?;
        self.writer
            .write(&batch)
            .map_err(|e| Error::Parquet(e.to_string()))?;
        self.buffer.clear();
        Ok(())
    }
}

impl ExportWriter for ParquetExportWriter {
    fn write_records(&mut self, records: &[TransactionRecord]) -> Result<(), Error> {
        for r in records {
            self.buffer.push(r.clone());
            if self.buffer.len() >= self.batch_size {
                self.flush_buffer()?;
            }
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<(), Error> {
        self.flush_buffer()?;
        self.writer
            .close()
            .map_err(|e| Error::Parquet(e.to_string()))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// CSV writer
// ---------------------------------------------------------------------------

/// Writes transaction records as CSV with JSON-encoded nested fields.
///
/// Nested fields (lists, structs) are serialized as JSON strings within
/// CSV cells, since CSV cannot represent nested data natively.
pub struct CsvExportWriter {
    inner: BufWriter<File>,
    header_written: bool,
}

impl CsvExportWriter {
    /// Create a new CSV writer at `path`.
    pub fn new(path: &Path) -> Result<Self, Error> {
        let file = File::create(path).map_err(Error::Io)?;
        Ok(Self {
            inner: BufWriter::new(file),
            header_written: false,
        })
    }

    fn write_header(&mut self) -> Result<(), Error> {
        if !self.header_written {
            writeln!(
                self.inner,
                "slot,block_time,signatures,accounts,program_ids,fee,\
                 account_balance_deltas,token_balance_deltas,log_messages,err,instructions"
            )
            .map_err(Error::Io)?;
            self.header_written = true;
        }
        Ok(())
    }
}

impl ExportWriter for CsvExportWriter {
    fn write_records(&mut self, records: &[TransactionRecord]) -> Result<(), Error> {
        self.write_header()?;
        for r in records {
            write!(self.inner, "{},", r.slot).map_err(Error::Io)?;
            match r.block_time {
                Some(t) => write!(self.inner, "{t},").map_err(Error::Io)?,
                None => write!(self.inner, ",").map_err(Error::Io)?,
            }
            write_csv_json(&mut self.inner, &r.signatures)?;
            write!(self.inner, ",").map_err(Error::Io)?;
            write_csv_json(&mut self.inner, &r.accounts)?;
            write!(self.inner, ",").map_err(Error::Io)?;
            write_csv_json(&mut self.inner, &r.program_ids)?;
            write!(self.inner, ",{},", r.fee).map_err(Error::Io)?;
            write_csv_json(&mut self.inner, &r.account_balance_deltas)?;
            write!(self.inner, ",").map_err(Error::Io)?;
            write_csv_json(&mut self.inner, &r.token_balance_deltas)?;
            write!(self.inner, ",").map_err(Error::Io)?;
            if let Some(logs) = &r.log_messages {
                write_csv_json(&mut self.inner, logs)?;
            }
            write!(self.inner, ",").map_err(Error::Io)?;
            if let Some(e) = &r.err {
                write_csv_escaped(&mut self.inner, e)?;
            }
            write!(self.inner, ",").map_err(Error::Io)?;
            write_csv_json(&mut self.inner, &r.instructions)?;
            writeln!(self.inner).map_err(Error::Io)?;
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<(), Error> {
        self.inner.flush().map_err(Error::Io)?;
        Ok(())
    }
}

/// Write a serde-serializable value as a quoted JSON string inside CSV.
fn write_csv_json<W: Write>(w: &mut W, value: &impl serde::Serialize) -> Result<(), Error> {
    let json = serde_json::to_string(value).map_err(Error::Json)?;
    // Double-quote the JSON and escape inner quotes
    write!(w, "\"{}\"", json.replace('"', "\"\"")).map_err(Error::Io)
}

/// Write a plain string value with CSV quoting.
fn write_csv_escaped<W: Write>(w: &mut W, value: &str) -> Result<(), Error> {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        write!(w, "\"{}\"", value.replace('"', "\"\"")).map_err(Error::Io)
    } else {
        write!(w, "{value}").map_err(Error::Io)
    }
}

// ---------------------------------------------------------------------------
// JSON Lines writer
// ---------------------------------------------------------------------------

/// Writes one JSON object per line (newline-delimited JSON).
pub struct JsonLinesExportWriter {
    inner: BufWriter<File>,
}

impl JsonLinesExportWriter {
    /// Create a new JSON Lines writer at `path`.
    pub fn new(path: &Path) -> Result<Self, Error> {
        let file = File::create(path).map_err(Error::Io)?;
        Ok(Self {
            inner: BufWriter::new(file),
        })
    }
}

impl ExportWriter for JsonLinesExportWriter {
    fn write_records(&mut self, records: &[TransactionRecord]) -> Result<(), Error> {
        for r in records {
            serde_json::to_writer(&mut self.inner, r).map_err(Error::Json)?;
            writeln!(self.inner).map_err(Error::Io)?;
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<(), Error> {
        self.inner.flush().map_err(Error::Io)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Partitioned writer
// ---------------------------------------------------------------------------

/// Demultiplexes records into separate files based on a partition key.
///
/// For decoded IDL output, the key is typically the event or instruction name.
/// Creates files like `{base_dir}/{key}.{ext}` on first encounter of each key.
pub struct PartitionedWriter {
    base_dir: PathBuf,
    format: OutputFormat,
    batch_size: usize,
    writers: HashMap<String, Box<dyn ExportWriter>>,
}

impl PartitionedWriter {
    /// Create a new partitioned writer that writes files under `base_dir`.
    pub fn new(base_dir: &Path, format: OutputFormat, batch_size: usize) -> Result<Self, Error> {
        fs::create_dir_all(base_dir).map_err(Error::Io)?;
        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            format,
            batch_size,
            writers: HashMap::new(),
        })
    }

    /// Write records grouped by a partition key extracted via `key_fn`.
    ///
    /// Records for the same key are batched together before writing.
    pub fn write_partitioned<F>(
        &mut self,
        records: &[TransactionRecord],
        key_fn: F,
    ) -> Result<(), Error>
    where
        F: Fn(&TransactionRecord) -> String,
    {
        let mut groups: HashMap<String, Vec<&TransactionRecord>> = HashMap::new();
        for r in records {
            groups.entry(key_fn(r)).or_default().push(r);
        }

        for (key, group) in &groups {
            let writer = self.get_or_create_writer(key)?;
            let owned: Vec<TransactionRecord> = group.iter().map(|r| (*r).clone()).collect();
            writer.write_records(&owned)?;
        }
        Ok(())
    }

    /// Finalize all partition writers.
    pub fn finish(self) -> Result<(), Error> {
        for (_, writer) in self.writers {
            writer.finish()?;
        }
        Ok(())
    }

    fn get_or_create_writer(&mut self, key: &str) -> Result<&mut Box<dyn ExportWriter>, Error> {
        if !self.writers.contains_key(key) {
            let ext = match self.format {
                OutputFormat::Parquet => "parquet",
                OutputFormat::Csv => "csv",
                OutputFormat::JsonLines => "jsonl",
            };
            let path = self.base_dir.join(format!("{key}.{ext}"));
            let writer: Box<dyn ExportWriter> = match self.format {
                OutputFormat::Parquet => {
                    Box::new(ParquetExportWriter::new(&path, self.batch_size)?)
                }
                OutputFormat::Csv => Box::new(CsvExportWriter::new(&path)?),
                OutputFormat::JsonLines => Box::new(JsonLinesExportWriter::new(&path)?),
            };
            self.writers.insert(key.to_string(), writer);
        }
        Ok(self.writers.get_mut(key).expect("just inserted"))
    }
}

// ---------------------------------------------------------------------------
// Factory helper
// ---------------------------------------------------------------------------

/// Create a single (non-partitioned) export writer for the given format.
pub fn create_writer(path: &Path, format: OutputFormat, batch_size: usize) -> Result<Box<dyn ExportWriter + Send>, Error> {
    match format {
        OutputFormat::Parquet => Ok(Box::new(ParquetExportWriter::new(path, batch_size)?)),
        OutputFormat::Csv => Ok(Box::new(CsvExportWriter::new(path)?)),
        OutputFormat::JsonLines => Ok(Box::new(JsonLinesExportWriter::new(path)?)),
    }
}

// ---------------------------------------------------------------------------
// Flat partitioned writer (for events / instructions extraction)
// ---------------------------------------------------------------------------

/// Maximum rows per partitioned output file before splitting to a new part.
const FLAT_PARTITION_MAX_ROWS: u64 = 500_000;

/// State for a single Parquet partition (one event/instruction name).
struct ParquetPartition {
    writer: ArrowWriter<File>,
    schema: SchemaRef,
    rows: u64,
    part: u32,
}

/// Writes [`FlatRecord`]s partitioned by `name` field into separate files.
///
/// Each partition (event name / instruction name) gets its own subdirectory
/// under `base_dir`, with files named `{epoch}_{name}_part_{NNN}.ext`.
/// Files are automatically split when they exceed 500,000 rows.
pub struct FlatPartitionedWriter {
    base_dir: PathBuf,
    epoch: u64,
    format: OutputFormat,
    schemas: HashMap<String, SchemaRef>,
    parquet_writers: HashMap<String, ParquetPartition>,
    jsonl_writers: HashMap<String, BufWriter<File>>,
}

impl FlatPartitionedWriter {
    /// Create a new flat partitioned writer.
    ///
    /// `schemas` maps event/instruction names to their Arrow schemas.
    /// `epoch` is used for file naming: `{epoch}_{name}_part_000.parquet`.
    pub fn new(
        base_dir: &Path,
        format: OutputFormat,
        schemas: HashMap<String, SchemaRef>,
        epoch: u64,
    ) -> Result<Self, Error> {
        fs::create_dir_all(base_dir).map_err(Error::Io)?;
        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            epoch,
            format,
            schemas,
            parquet_writers: HashMap::new(),
            jsonl_writers: HashMap::new(),
        })
    }

    /// Write a batch of flat records, routing each to its partition by `name`.
    pub fn write_records(&mut self, records: &[FlatRecord]) -> Result<(), Error> {
        let mut groups: HashMap<&str, Vec<&FlatRecord>> = HashMap::new();
        for r in records {
            groups.entry(r.name.as_str()).or_default().push(r);
        }

        for (name, group) in groups {
            match self.format {
                OutputFormat::Parquet => {
                    let schema = match self.schemas.get(name) {
                        Some(s) => Arc::clone(s),
                        None => continue,
                    };

                    if !self.parquet_writers.contains_key(name) {
                        let partition =
                            self.create_parquet_partition(name, &schema, 0)?;
                        self.parquet_writers.insert(name.to_string(), partition);
                    }

                    let partition =
                        self.parquet_writers.get_mut(name).expect("just inserted");
                    let owned: Vec<FlatRecord> = group.into_iter().cloned().collect();
                    let batch = flat_records_to_batch(&partition.schema, &owned)?;
                    partition
                        .writer
                        .write(&batch)
                        .map_err(|e| Error::Parquet(e.to_string()))?;
                    partition.rows += batch.num_rows() as u64;

                    // Split to a new part file if we exceeded the row limit.
                    if partition.rows >= FLAT_PARTITION_MAX_ROWS {
                        let next_part = partition.part + 1;
                        let old = self.parquet_writers.remove(name).unwrap();
                        old.writer
                            .close()
                            .map_err(|e| Error::Parquet(e.to_string()))?;
                        let new_partition =
                            self.create_parquet_partition(name, &schema, next_part)?;
                        self.parquet_writers.insert(name.to_string(), new_partition);
                    }
                }
                OutputFormat::JsonLines | OutputFormat::Csv => {
                    if !self.jsonl_writers.contains_key(name) {
                        let w = self.create_text_writer(name)?;
                        self.jsonl_writers.insert(name.to_string(), w);
                    }
                    let w = self.jsonl_writers.get_mut(name).expect("just inserted");
                    for r in &group {
                        serde_json::to_writer(&mut *w, r).map_err(Error::Json)?;
                        writeln!(w).map_err(Error::Io)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Finalize all partition writers.
    pub fn finish(self) -> Result<(), Error> {
        for (_, partition) in self.parquet_writers {
            partition
                .writer
                .close()
                .map_err(|e| Error::Parquet(e.to_string()))?;
        }
        for (_, mut w) in self.jsonl_writers {
            w.flush().map_err(Error::Io)?;
        }
        Ok(())
    }

    fn partition_dir(&self, name: &str) -> PathBuf {
        self.base_dir.join(name)
    }

    fn create_parquet_partition(
        &self,
        name: &str,
        schema: &SchemaRef,
        part: u32,
    ) -> Result<ParquetPartition, Error> {
        let dir = self.partition_dir(name);
        fs::create_dir_all(&dir).map_err(Error::Io)?;
        let path = dir.join(format!(
            "{}_{}_part_{:03}.parquet",
            self.epoch, name, part
        ));
        let file = File::create(&path).map_err(Error::Io)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(3).expect("valid zstd level"),
            ))
            .build();
        let writer = ArrowWriter::try_new(file, Arc::clone(schema), Some(props))
            .map_err(|e| Error::Parquet(e.to_string()))?;
        Ok(ParquetPartition {
            writer,
            schema: Arc::clone(schema),
            rows: 0,
            part,
        })
    }

    fn create_text_writer(&self, name: &str) -> Result<BufWriter<File>, Error> {
        let dir = self.partition_dir(name);
        fs::create_dir_all(&dir).map_err(Error::Io)?;
        let ext = match self.format {
            OutputFormat::JsonLines => "jsonl",
            OutputFormat::Csv => "csv",
            _ => unreachable!(),
        };
        let path = dir.join(format!(
            "{}_{}_part_000.{ext}",
            self.epoch, name
        ));
        let file = File::create(&path).map_err(Error::Io)?;
        Ok(BufWriter::new(file))
    }
}
