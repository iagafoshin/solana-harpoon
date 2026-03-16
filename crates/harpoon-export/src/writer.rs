//! Export writers: Parquet (ZSTD), CSV, JSON Lines.
//!
//! All writers implement the [`ExportWriter`] trait for a uniform streaming API.
//! The [`PartitionedWriter`] demultiplexes records by a key (e.g. event name)
//! into separate files.

use {
    crate::{
        record::TransactionRecord,
        schema::{build_schema, records_to_batch},
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
pub fn create_writer(path: &Path, format: OutputFormat, batch_size: usize) -> Result<Box<dyn ExportWriter>, Error> {
    match format {
        OutputFormat::Parquet => Ok(Box::new(ParquetExportWriter::new(path, batch_size)?)),
        OutputFormat::Csv => Ok(Box::new(CsvExportWriter::new(path)?)),
        OutputFormat::JsonLines => Ok(Box::new(JsonLinesExportWriter::new(path)?)),
    }
}
