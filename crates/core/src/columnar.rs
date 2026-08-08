//! Opt-in Apache Arrow columnar record path (feature `arrow`, RFC 0002 / #375).
//!
//! This is the **escape hatch** described in #324: a columnar representation a
//! connector may produce or consume at the page boundary, *additive* to and
//! coexisting with the default `serde_json::Value` row model. It touches neither
//! [`StreamPage`](crate::StreamPage) nor any existing connector — an Arrow-native
//! source overrides [`Source::stream_batches`](crate::Source::stream_batches) and
//! an Arrow-native sink overrides
//! [`Sink::write_batch_columnar`](crate::Sink::write_batch_columnar); the pipeline
//! uses the columnar path only when **both** sides advertise support (and no
//! `Value`-shaped stage needs to observe the records), so a
//! `parquet → parquet` chain never materializes `Value`.
//!
//! The `RecordBatch ↔ Value` conversions here are the single source of truth for
//! the shim (they match `faucet-transform-sql`'s `shovel` byte-for-byte, incl.
//! `with_explicit_nulls(true)` so an explicit-null field round-trips as
//! `"key": null` rather than being silently dropped — audit #321 H6).

use crate::FaucetError;
use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use serde_json::Value;
use std::sync::Arc;

/// A page of records in **columnar** (Arrow) form, the columnar analogue of
/// [`StreamPage`](crate::StreamPage).
///
/// `bookmark` carries the exact same checkpoint semantics as `StreamPage`:
/// `Some` triggers flush + bookmark-persist after the batch is durably written;
/// most sources emit `Some` only on the final batch, CDC-style sources per
/// committed transaction.
#[derive(Debug, Clone)]
pub struct ColumnarPage {
    /// The record batch to write to the sink for this page.
    pub batch: RecordBatch,
    /// Optional bookmark to checkpoint after this batch is durably written.
    pub bookmark: Option<Value>,
}

impl ColumnarPage {
    /// Construct a columnar page from a batch and optional bookmark.
    pub fn new(batch: RecordBatch, bookmark: Option<Value>) -> Self {
        Self { batch, bookmark }
    }

    /// Number of rows in the batch.
    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }
}

/// Map any display-able error into a [`FaucetError::Transform`] with context.
fn te<E: std::fmt::Display>(ctx: &str, e: E) -> FaucetError {
    FaucetError::Transform(format!("columnar shim: {ctx}: {e}"))
}

/// Infer an Arrow [`Schema`] from a slice of JSON records (each a JSON object).
///
/// Returns the inferred schema in an [`Arc`]. An empty slice yields a schema
/// with no fields.
pub fn infer_arrow_schema(records: &[Value]) -> Result<SchemaRef, FaucetError> {
    let iter = records
        .iter()
        .map(|v| Ok::<_, arrow::error::ArrowError>(v.clone()));
    let schema = arrow_json::reader::infer_json_schema_from_iterator(iter)
        .map_err(|e| te("schema inference", e))?;
    refine_wide_integers(&schema, records).map(Arc::new)
}

/// Re-type fields that `arrow-json` widened to `Float64` purely because an
/// integer did not fit `i64`, and refuse the cases that cannot be re-typed.
///
/// `arrow-json` infers `Float64` for any integer outside `i64` range, so a `u64`
/// id such as `18446744073709551615` silently becomes `1.8446744073709552e19` —
/// exact value gone, `Ok` returned (#460). Where every observed value for such a
/// field is a non-negative integer, `UInt64` holds it exactly, so use that.
/// Where no integer type fits (values spanning negative *and* above `i64::MAX`)
/// or the value sits somewhere this pass does not re-type (inside a list), fail
/// with a typed error naming the path rather than approximating it.
///
/// Fields arrow-json typed `Float64` because a value genuinely *is* fractional
/// are left alone, as is a field mixing integers and floats — coercing those to
/// `Float64` is ordinary JSON-number behaviour, not loss of an exact integer.
fn refine_wide_integers(schema: &Schema, records: &[Value]) -> Result<Schema, FaucetError> {
    use arrow::datatypes::{DataType, Field};

    /// Values observed at one field across the page (nulls skipped).
    fn observed<'a>(records: &'a [Value], name: &str) -> Vec<&'a Value> {
        records
            .iter()
            .filter_map(|r| r.get(name))
            .filter(|v| !v.is_null())
            .collect()
    }

    /// Does this number need more than `i64` *and* is it an exact integer?
    fn is_wide_integer(v: &Value) -> bool {
        matches!(v, Value::Number(n) if !n.is_i64() && n.is_u64())
    }

    fn refine_field(field: &Field, values: Vec<&Value>) -> Result<Field, FaucetError> {
        match field.data_type() {
            DataType::Float64 if values.iter().any(|v| is_wide_integer(v)) => {
                // Every value integral and non-negative → UInt64 is exact.
                if values
                    .iter()
                    .all(|v| matches!(v, Value::Number(n) if n.is_u64()))
                {
                    Ok(field.clone().with_data_type(DataType::UInt64))
                } else {
                    Err(FaucetError::Transform(format!(
                        "columnar shim: field {:?} mixes an integer above i64::MAX with values \
                         no unsigned type can hold, so no exact Arrow type fits. Convert it to a \
                         string first (a `cast` transform) — it is not silently widened to a \
                         float because that loses the exact value",
                        field.name()
                    )))
                }
            }
            DataType::Struct(children) => {
                let refined = children
                    .iter()
                    .map(|child| {
                        let child_values = values
                            .iter()
                            .filter_map(|v| v.get(child.name()))
                            .filter(|v| !v.is_null())
                            .collect();
                        refine_field(child, child_values).map(Arc::new)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(field
                    .clone()
                    .with_data_type(DataType::Struct(refined.into())))
            }
            // A wide integer inside a list is not re-typed by this pass; refusing
            // beats writing an approximation nobody asked for.
            _ if contains_wide_integer_in_container(&values) => {
                Err(FaucetError::Transform(format!(
                    "columnar shim: field {:?} contains an integer above i64::MAX inside a list, \
                     which the columnar path cannot represent exactly. Convert those elements to \
                     strings first (a `cast` transform)",
                    field.name()
                )))
            }
            _ => Ok(field.clone()),
        }
    }

    /// Any wide integer nested inside an array (at any depth).
    fn contains_wide_integer_in_container(values: &[&Value]) -> bool {
        fn walk(v: &Value, in_list: bool) -> bool {
            match v {
                Value::Array(items) => items.iter().any(|i| walk(i, true)),
                Value::Object(map) => map.values().any(|i| walk(i, in_list)),
                other => in_list && is_wide_integer(other),
            }
        }
        values.iter().any(|v| walk(v, false))
    }

    let fields = schema
        .fields()
        .iter()
        .map(|f| refine_field(f, observed(records, f.name())).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Schema::new(fields).with_metadata(schema.metadata().clone()))
}

/// Encode a slice of JSON records into a single [`RecordBatch`] against `schema`.
///
/// Returns an empty batch if `records` is empty.
pub fn values_to_record_batch(
    records: &[Value],
    schema: SchemaRef,
) -> Result<RecordBatch, FaucetError> {
    let mut decoder = arrow_json::ReaderBuilder::new(schema.clone())
        .build_decoder()
        .map_err(|e| te("decoder build", e))?;
    decoder.serialize(records).map_err(|e| te("encode", e))?;
    let mut batches = Vec::new();
    while let Some(b) = decoder.flush().map_err(|e| te("flush", e))? {
        batches.push(b);
    }
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    if batches.len() == 1 {
        return Ok(batches.pop().unwrap());
    }
    arrow::compute::concat_batches(&schema, &batches).map_err(|e| te("concat", e))
}

/// Convenience: infer the schema from `records` and encode them into a batch.
pub fn values_to_record_batch_inferred(records: &[Value]) -> Result<RecordBatch, FaucetError> {
    let schema = infer_arrow_schema(records)?;
    values_to_record_batch(records, schema)
}

/// Decode a [`RecordBatch`] into JSON objects (one per row).
///
/// Uses `arrow-json`'s array writer with **explicit nulls enabled**, so a
/// null-valued column is emitted as `"key": null` rather than omitted — without
/// this a `SELECT *`-style identity would silently delete every explicit-null
/// field (audit #321 H6). An empty batch returns an empty `Vec`.
pub fn record_batch_to_values(batch: &RecordBatch) -> Result<Vec<Value>, FaucetError> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow_json::writer::WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, arrow_json::writer::JsonArray>(&mut buf);
        writer.write(batch).map_err(|e| te("json write", e))?;
        writer.finish().map_err(|e| te("json finish", e))?;
    }
    serde_json::from_slice(&buf).map_err(|e| te("json parse", e))
}

/// Compare two schemas for field-level equality (name + data-type + nullability).
pub fn schema_eq(a: &Schema, b: &Schema) -> bool {
    a.fields() == b.fields()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trip_scalars_nulls_nested() {
        let recs = vec![
            json!({"id": 1, "name": "a", "score": 1.5, "ok": true, "tags": ["x", "y"]}),
            json!({"id": 2, "name": null, "score": null, "ok": false, "tags": []}),
        ];
        let batch = values_to_record_batch_inferred(&recs).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let back = record_batch_to_values(&batch).unwrap();
        assert_eq!(back[0]["id"], json!(1));
        assert_eq!(back[0]["tags"], json!(["x", "y"]));
        // #321 H6: an explicit-null field survives the round-trip.
        assert!(back[1].as_object().unwrap().contains_key("name"));
        assert_eq!(back[1]["name"], json!(null));
    }

    #[test]
    fn empty_records_yield_empty_batch_and_back() {
        let schema = infer_arrow_schema(&[json!({"a": 1})]).unwrap();
        let batch = values_to_record_batch(&[], schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert!(record_batch_to_values(&batch).unwrap().is_empty());
    }

    #[test]
    fn columnar_page_reports_rows() {
        let batch = values_to_record_batch_inferred(&[json!({"a": 1}), json!({"a": 2})]).unwrap();
        let page = ColumnarPage::new(batch, Some(json!({"lsn": 42})));
        assert_eq!(page.num_rows(), 2);
        assert_eq!(page.bookmark, Some(json!({"lsn": 42})));
    }
}

#[cfg(test)]
mod wide_integer_tests {
    use super::*;
    use serde_json::json;

    /// #460: arrow-json types an integer above `i64::MAX` as `Float64`, so
    /// `18446744073709551615` used to come back as `1.8446744073709552e19` with
    /// `Ok`. It must now round-trip exactly.
    #[test]
    fn u64_above_i64_max_round_trips_exactly() {
        let recs = vec![json!({"id": u64::MAX})];
        let back =
            record_batch_to_values(&values_to_record_batch_inferred(&recs).unwrap()).unwrap();
        assert_eq!(back[0]["id"], json!(u64::MAX), "exact value must survive");
        assert!(back[0]["id"].is_u64(), "and stay an integer, not a float");
    }

    #[test]
    fn mixed_small_and_wide_integers_all_survive() {
        let recs = vec![
            json!({"id": 1}),
            json!({"id": u64::MAX}),
            json!({"id": null}),
        ];
        let back =
            record_batch_to_values(&values_to_record_batch_inferred(&recs).unwrap()).unwrap();
        assert_eq!(back[0]["id"], json!(1));
        assert_eq!(back[1]["id"], json!(u64::MAX));
        assert_eq!(back[2]["id"], json!(null));
    }

    #[test]
    fn wide_integer_nested_in_a_struct_survives() {
        let recs = vec![json!({"outer": {"id": u64::MAX, "n": 3}})];
        let back =
            record_batch_to_values(&values_to_record_batch_inferred(&recs).unwrap()).unwrap();
        assert_eq!(back[0]["outer"]["id"], json!(u64::MAX));
        assert_eq!(back[0]["outer"]["n"], json!(3));
    }

    /// Genuine floats, and integer/float mixes, keep their existing behaviour —
    /// coercing those to Float64 is ordinary JSON-number semantics, not loss of
    /// an exact integer.
    #[test]
    fn genuine_floats_are_untouched() {
        let recs = vec![json!({"a": 1.5}), json!({"a": 2})];
        let back =
            record_batch_to_values(&values_to_record_batch_inferred(&recs).unwrap()).unwrap();
        assert_eq!(back[0]["a"], json!(1.5));
        assert_eq!(back[1]["a"], json!(2.0));

        // i64 range is unaffected (2^53+1 already round-tripped before).
        let recs = vec![json!({"n": 9007199254740993i64, "m": i64::MIN})];
        let back =
            record_batch_to_values(&values_to_record_batch_inferred(&recs).unwrap()).unwrap();
        assert_eq!(back[0]["n"], json!(9007199254740993i64));
        assert_eq!(back[0]["m"], json!(i64::MIN));
    }

    /// No exact type fits a field spanning negatives and above-i64::MAX, so it
    /// must fail loudly rather than approximate.
    #[test]
    fn unrepresentable_mix_is_refused() {
        let recs = vec![json!({"v": -1}), json!({"v": u64::MAX})];
        let err = match values_to_record_batch_inferred(&recs) {
            Err(e) => e.to_string(),
            Ok(b) => panic!("must refuse; got {:?}", record_batch_to_values(&b).unwrap()),
        };
        assert!(err.contains("\"v\""), "{err}");
        assert!(err.contains("cast"), "points at the workaround: {err}");
    }

    /// A wide integer inside a list is not re-typed by this pass, so it is
    /// refused rather than silently written as a float.
    #[test]
    fn wide_integer_inside_a_list_is_refused() {
        let recs = vec![json!({"ids": [1, u64::MAX]})];
        let err = match values_to_record_batch_inferred(&recs) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("must refuse a wide integer inside a list"),
        };
        assert!(err.contains("\"ids\""), "{err}");
        assert!(err.contains("list"), "{err}");
    }
}
