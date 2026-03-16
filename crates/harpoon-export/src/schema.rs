//! Arrow schema definitions and record-batch construction.
//!
//! Migrated from `solana_extractor.rs` — `build_schema()` + `flush_batch_sync()`.

use {
    crate::{
        record::{FlatRecord, TransactionRecord},
        Error,
    },
    arrow::{
        array::{
            ArrayBuilder, ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder,
            Int32Builder, Int64Builder, Int8Builder, ListBuilder, RecordBatch, StringBuilder,
            StructBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
        },
        datatypes::{DataType, Field, Fields, Schema, SchemaRef},
    },
    std::sync::Arc,
};

/// Build the default Arrow schema for raw transaction output.
///
/// This matches the "Output schema" from ARCHITECTURE.md §6.
#[must_use]
pub fn build_schema() -> SchemaRef {
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

/// Convert a batch of [`TransactionRecord`]s into an Arrow [`RecordBatch`].
pub fn records_to_batch(
    schema: &SchemaRef,
    records: &[TransactionRecord],
) -> Result<RecordBatch, Error> {
    if records.is_empty() {
        return RecordBatch::try_new(Arc::clone(schema), empty_arrays(schema))
            .map_err(Error::Arrow);
    }

    let rows = records.len();

    let mut slot_b = UInt64Builder::with_capacity(rows);
    let mut block_time_b = Int64Builder::with_capacity(rows);
    let mut sigs_b = ListBuilder::with_capacity(StringBuilder::new(), rows);
    let mut accounts_b = ListBuilder::with_capacity(StringBuilder::new(), rows);
    let mut program_ids_b = ListBuilder::with_capacity(StringBuilder::new(), rows);
    let mut fee_b = UInt64Builder::with_capacity(rows);

    let mut ab_b = ListBuilder::with_capacity(
        StructBuilder::new(
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
        ),
        rows,
    );

    let mut tb_b = ListBuilder::with_capacity(
        StructBuilder::new(
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
        ),
        rows,
    );

    let mut ix_b = ListBuilder::with_capacity(
        StructBuilder::new(
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
        ),
        rows,
    );

    let mut logs_b = ListBuilder::with_capacity(StringBuilder::new(), rows);
    let mut err_b = StringBuilder::with_capacity(rows, rows.saturating_mul(16));

    for r in records {
        slot_b.append_value(r.slot);
        match r.block_time {
            Some(v) => block_time_b.append_value(v),
            None => block_time_b.append_null(),
        }

        append_string_list(&mut sigs_b, &r.signatures);
        append_string_list(&mut accounts_b, &r.accounts);
        append_string_list(&mut program_ids_b, &r.program_ids);
        fee_b.append_value(r.fee);

        // account balance deltas
        let ab_vals = ab_b.values();
        for d in &r.account_balance_deltas {
            ab_vals
                .field_builder::<StringBuilder>(0)
                .expect("account field")
                .append_value(&d.account);
            ab_vals
                .field_builder::<UInt64Builder>(1)
                .expect("pre_lamports field")
                .append_value(d.pre_lamports);
            ab_vals
                .field_builder::<UInt64Builder>(2)
                .expect("post_lamports field")
                .append_value(d.post_lamports);
            ab_vals
                .field_builder::<Int64Builder>(3)
                .expect("delta_lamports field")
                .append_value(d.delta_lamports);
            ab_vals.append(true);
        }
        ab_b.append(true);

        // token balance deltas
        let tb_vals = tb_b.values();
        for d in &r.token_balance_deltas {
            tb_vals
                .field_builder::<StringBuilder>(0)
                .expect("mint field")
                .append_value(&d.mint);
            tb_vals
                .field_builder::<StringBuilder>(1)
                .expect("owner field")
                .append_value(&d.owner);
            tb_vals
                .field_builder::<UInt32Builder>(2)
                .expect("account_index field")
                .append_value(d.account_index);
            match &d.pre_ui_amount {
                Some(v) => tb_vals
                    .field_builder::<StringBuilder>(3)
                    .expect("pre_ui_amount field")
                    .append_value(v),
                None => tb_vals
                    .field_builder::<StringBuilder>(3)
                    .expect("pre_ui_amount field")
                    .append_null(),
            }
            match &d.post_ui_amount {
                Some(v) => tb_vals
                    .field_builder::<StringBuilder>(4)
                    .expect("post_ui_amount field")
                    .append_value(v),
                None => tb_vals
                    .field_builder::<StringBuilder>(4)
                    .expect("post_ui_amount field")
                    .append_null(),
            }
            tb_vals.append(true);
        }
        tb_b.append(true);

        // instructions
        let ix_vals = ix_b.values();
        for inst in &r.instructions {
            ix_vals
                .field_builder::<StringBuilder>(0)
                .expect("program_id field")
                .append_value(&inst.program_id);
            ix_vals
                .field_builder::<StringBuilder>(1)
                .expect("data field")
                .append_value(&inst.data);
            let accs = ix_vals
                .field_builder::<ListBuilder<StringBuilder>>(2)
                .expect("accounts field");
            let accs_vals = accs.values();
            for a in &inst.accounts {
                accs_vals.append_value(a);
            }
            accs.append(true);
            ix_vals.append(true);
        }
        ix_b.append(true);

        // log messages
        match &r.log_messages {
            Some(logs) => {
                let vals = logs_b.values();
                for l in logs {
                    vals.append_value(l);
                }
                logs_b.append(true);
            }
            None => logs_b.append(false),
        }

        match &r.err {
            Some(e) => err_b.append_value(e),
            None => err_b.append_null(),
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(slot_b.finish()),
        Arc::new(block_time_b.finish()),
        Arc::new(sigs_b.finish()),
        Arc::new(accounts_b.finish()),
        Arc::new(program_ids_b.finish()),
        Arc::new(fee_b.finish()),
        Arc::new(ab_b.finish()),
        Arc::new(tb_b.finish()),
        Arc::new(logs_b.finish()),
        Arc::new(err_b.finish()),
        Arc::new(ix_b.finish()),
    ];

    RecordBatch::try_new(Arc::clone(schema), arrays).map_err(Error::Arrow)
}

fn append_string_list(builder: &mut ListBuilder<StringBuilder>, values: &[String]) {
    let vals = builder.values();
    for v in values {
        vals.append_value(v);
    }
    builder.append(true);
}

fn empty_arrays(schema: &SchemaRef) -> Vec<ArrayRef> {
    schema
        .fields()
        .iter()
        .map(|f| arrow::array::new_empty_array(f.data_type()))
        .collect()
}

// ---------------------------------------------------------------------------
// Flat record schema (for events / instructions extraction)
// ---------------------------------------------------------------------------

const FLAT_COMMON_COUNT: usize = 5;

/// Common fields for flat event/instruction records.
fn flat_common_fields() -> Vec<Field> {
    vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("block_time", DataType::Int64, true),
        Field::new("signature", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("program_id", DataType::Utf8, false),
    ]
}

/// Build a flat Arrow schema from common fields + dynamic fields.
#[must_use]
pub fn build_flat_schema(extra_fields: &[Field]) -> SchemaRef {
    let mut fields = flat_common_fields();
    fields.extend_from_slice(extra_fields);
    Arc::new(Schema::new(fields))
}

/// Convert flat records to an Arrow [`RecordBatch`] using the given schema.
pub fn flat_records_to_batch(
    schema: &SchemaRef,
    records: &[FlatRecord],
) -> Result<RecordBatch, Error> {
    if records.is_empty() {
        return RecordBatch::try_new(Arc::clone(schema), empty_arrays(schema))
            .map_err(Error::Arrow);
    }

    let rows = records.len();
    let mut slot_b = UInt64Builder::with_capacity(rows);
    let mut block_time_b = Int64Builder::with_capacity(rows);
    let mut sig_b = StringBuilder::new();
    let mut name_b = StringBuilder::new();
    let mut pid_b = StringBuilder::new();

    let dynamic_fields: Vec<_> = schema.fields()[FLAT_COMMON_COUNT..].to_vec();
    let mut dynamic_builders: Vec<DynamicBuilder> = dynamic_fields
        .iter()
        .map(|f| DynamicBuilder::new(f.data_type(), rows))
        .collect();

    for r in records {
        slot_b.append_value(r.slot);
        match r.block_time {
            Some(v) => block_time_b.append_value(v),
            None => block_time_b.append_null(),
        }
        sig_b.append_value(&r.signature);
        name_b.append_value(&r.name);
        pid_b.append_value(&r.program_id);

        for (i, field) in dynamic_fields.iter().enumerate() {
            dynamic_builders[i].append_json(r.fields.get(field.name().as_str()));
        }
    }

    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(slot_b.finish()),
        Arc::new(block_time_b.finish()),
        Arc::new(sig_b.finish()),
        Arc::new(name_b.finish()),
        Arc::new(pid_b.finish()),
    ];

    for b in dynamic_builders {
        arrays.push(b.finish());
    }

    RecordBatch::try_new(Arc::clone(schema), arrays).map_err(Error::Arrow)
}

/// Type-erased Arrow array builder that converts `serde_json::Value` to typed arrays.
enum DynamicBuilder {
    UInt8(UInt8Builder),
    UInt16(UInt16Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Utf8(StringBuilder),
}

impl DynamicBuilder {
    fn new(dt: &DataType, capacity: usize) -> Self {
        match dt {
            DataType::UInt8 => Self::UInt8(UInt8Builder::with_capacity(capacity)),
            DataType::UInt16 => Self::UInt16(UInt16Builder::with_capacity(capacity)),
            DataType::UInt32 => Self::UInt32(UInt32Builder::with_capacity(capacity)),
            DataType::UInt64 => Self::UInt64(UInt64Builder::with_capacity(capacity)),
            DataType::Int8 => Self::Int8(Int8Builder::with_capacity(capacity)),
            DataType::Int16 => Self::Int16(Int16Builder::with_capacity(capacity)),
            DataType::Int32 => Self::Int32(Int32Builder::with_capacity(capacity)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float32 => Self::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            _ => Self::Utf8(StringBuilder::new()),
        }
    }

    fn append_json(&mut self, value: Option<&serde_json::Value>) {
        match self {
            Self::UInt8(b) => match value.and_then(|v| v.as_u64()) {
                Some(n) => b.append_value(n as u8),
                None => b.append_null(),
            },
            Self::UInt16(b) => match value.and_then(|v| v.as_u64()) {
                Some(n) => b.append_value(n as u16),
                None => b.append_null(),
            },
            Self::UInt32(b) => match value.and_then(|v| v.as_u64()) {
                Some(n) => b.append_value(n as u32),
                None => b.append_null(),
            },
            Self::UInt64(b) => match value.and_then(|v| v.as_u64()) {
                Some(n) => b.append_value(n),
                None => b.append_null(),
            },
            Self::Int8(b) => match value.and_then(|v| v.as_i64()) {
                Some(n) => b.append_value(n as i8),
                None => b.append_null(),
            },
            Self::Int16(b) => match value.and_then(|v| v.as_i64()) {
                Some(n) => b.append_value(n as i16),
                None => b.append_null(),
            },
            Self::Int32(b) => match value.and_then(|v| v.as_i64()) {
                Some(n) => b.append_value(n as i32),
                None => b.append_null(),
            },
            Self::Int64(b) => match value.and_then(|v| v.as_i64()) {
                Some(n) => b.append_value(n),
                None => b.append_null(),
            },
            Self::Float32(b) => match value.and_then(|v| v.as_f64()) {
                Some(n) => b.append_value(n as f32),
                None => b.append_null(),
            },
            Self::Float64(b) => match value.and_then(|v| v.as_f64()) {
                Some(n) => b.append_value(n),
                None => b.append_null(),
            },
            Self::Boolean(b) => match value.and_then(|v| v.as_bool()) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            Self::Utf8(b) => match value {
                Some(serde_json::Value::String(s)) => b.append_value(s),
                Some(v) => b.append_value(v.to_string()),
                None => b.append_null(),
            },
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::UInt8(mut b) => Arc::new(b.finish()),
            Self::UInt16(mut b) => Arc::new(b.finish()),
            Self::UInt32(mut b) => Arc::new(b.finish()),
            Self::UInt64(mut b) => Arc::new(b.finish()),
            Self::Int8(mut b) => Arc::new(b.finish()),
            Self::Int16(mut b) => Arc::new(b.finish()),
            Self::Int32(mut b) => Arc::new(b.finish()),
            Self::Int64(mut b) => Arc::new(b.finish()),
            Self::Float32(mut b) => Arc::new(b.finish()),
            Self::Float64(mut b) => Arc::new(b.finish()),
            Self::Boolean(mut b) => Arc::new(b.finish()),
            Self::Utf8(mut b) => Arc::new(b.finish()),
        }
    }
}
