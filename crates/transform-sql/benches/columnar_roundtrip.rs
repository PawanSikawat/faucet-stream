//! Columnar-chain benchmark for issue #324 (D): measure the `serde_json::Value`
//! round-trip cost on a `parquet → transform-sql(sql) → parquet` chain, and
//! attribute it against the irreducible work (Parquet encode/decode + DuckDB SQL).
//!
//! faucet's page interchange type is row-wise `Vec<Value>` (`StreamPage.records`).
//! On a columnar chain each boundary therefore pays an Arrow↔JSON `Value`
//! conversion that a fully Arrow-native fast path would skip:
//!
//! ```text
//!  Parquet bytes ──decode──▶ RecordBatch ──[arrow→value]──▶ Vec<Value>   (source)
//!  Vec<Value> ──[value→arrow]──▶ RecordBatch ──DuckDB SQL──▶ RecordBatch
//!             ──[arrow→value]──▶ Vec<Value>                              (transform)
//!  Vec<Value> ──[value→arrow]──▶ RecordBatch ──encode──▶ Parquet bytes  (sink)
//! ```
//!
//! The four `[..]`-tagged conversions are the "Value tax." Everything else
//! (Parquet encode/decode, DuckDB) is irreducible. This bench times each
//! primitive so the tax can be expressed as a fraction of a real chain — the
//! number the opt-in-Arrow-fast-path go/no-go hinges on.
//!
//! It also carries an **S3-bulk variant** (`s3_bulk/*`). S3's bulk interchange
//! is JSONL, not Arrow, so its tax is `Value` ↔ JSON *text* (mirroring the real
//! `faucet-sink-s3` / `faucet-source-s3` encoders). Comparing its round-trip to
//! the Parquet primitives quantifies the saving an Arrow-native Parquet-on-S3
//! path would capture for bulk analytical objects.
//!
//! The Arrow↔Value conversions below are byte-identical to `src/shovel.rs`
//! (same `arrow_json` builders, `with_explicit_nulls(true)` on the writer) so
//! the measurement reflects the real code path, not an approximation.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use faucet_core::stage::{apply_stages_to_page, compile_stage};
use faucet_transform_sql::{SqlTransform, SqlTransformConfig};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Value, json};
use std::sync::Arc;

// ── Arrow ↔ Value (identical to src/shovel.rs) ──────────────────────────────

fn infer_schema(records: &[Value]) -> SchemaRef {
    let iter = records
        .iter()
        .map(|v| Ok::<_, arrow::error::ArrowError>(v.clone()));
    Arc::new(arrow_json::reader::infer_json_schema_from_iterator(iter).unwrap())
}

fn json_to_record_batch(records: &[Value], schema: SchemaRef) -> RecordBatch {
    let mut decoder = arrow_json::ReaderBuilder::new(schema.clone())
        .build_decoder()
        .unwrap();
    decoder.serialize(records).unwrap();
    let mut batches = Vec::new();
    while let Some(b) = decoder.flush().unwrap() {
        batches.push(b);
    }
    if batches.is_empty() {
        return RecordBatch::new_empty(schema);
    }
    if batches.len() == 1 {
        return batches.pop().unwrap();
    }
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

fn record_batch_to_json(batch: &RecordBatch) -> Vec<Value> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow_json::writer::WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, arrow_json::writer::JsonArray>(&mut buf);
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    serde_json::from_slice(&buf).unwrap()
}

// ── Parquet encode / decode (in-memory, one page = one row group) ────────────

fn parquet_encode(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    buf
}

fn parquet_decode(bytes: Vec<u8>) -> RecordBatch {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    let schema = batches[0].schema();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

// ── S3-bulk JSONL encode / decode (identical to the S3 sink/source) ──────────
//
// S3's bulk interchange is JSONL, not Arrow — so the tax here is `Value` ↔ JSON
// *text*, and the Arrow-native proposition for S3 is "store Parquet objects
// instead" (measured by the `work/parquet_*` primitives above). These two fns
// mirror `faucet-sink-s3::serialize_jsonl` (per-record `to_vec` + `\n`) and
// `faucet-source-s3`'s line reader (`lines()` → `from_str` per line).

fn s3_jsonl_encode(records: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();
    for record in records {
        let line = serde_json::to_vec(record).unwrap();
        buf.extend_from_slice(&line);
        buf.push(b'\n');
    }
    buf
}

fn s3_jsonl_decode(bytes: &[u8]) -> Vec<Value> {
    let text = std::str::from_utf8(bytes).unwrap();
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str::<Value>(l).unwrap())
        .collect()
}

// ── Representative analytical page: mixed typed columns ──────────────────────

fn page(n: usize) -> Vec<Value> {
    const REGIONS: [&str; 4] = ["NA", "EU", "APAC", "LATAM"];
    (0..n)
        .map(|i| {
            let region = REGIONS[i % 4];
            json!({
                "id": i as i64,
                "region": region,
                "amount": (i as f64) * 1.5,
                "qty": (i % 100) as i64,
                "active": i % 2 == 0,
                "ts": 1_700_000_000_i64 + i as i64,
            })
        })
        .collect()
}

fn sql_stage(query: &str) -> faucet_core::stage::CompiledStage {
    let cfg = SqlTransformConfig {
        query: query.into(),
        relations: vec![],
        memory_limit: None,
        threads: Some(1),
    };
    compile_stage(&SqlTransform::compile(&cfg).unwrap().into_page_stage()).unwrap()
}

fn bench(c: &mut Criterion) {
    for &n in &[1_000usize, 10_000, 50_000] {
        let recs = page(n);
        let schema = infer_schema(&recs);
        let batch = json_to_record_batch(&recs, schema.clone());
        let encoded = parquet_encode(&batch);

        // ── The Value tax: the conversions an Arrow-native path would skip ──
        // arrow → value (parquet source emits Vec<Value>; sql transform re-emits).
        c.bench_function(&format!("tax/arrow_to_value/{n}"), |b| {
            b.iter(|| record_batch_to_json(std::hint::black_box(&batch)))
        });
        // value → arrow (sql transform feeds DuckDB; parquet sink writes).
        c.bench_function(&format!("tax/value_to_arrow/{n}"), |b| {
            b.iter(|| json_to_record_batch(std::hint::black_box(&recs), schema.clone()))
        });
        // One full boundary round-trip (arrow → value → arrow).
        c.bench_function(&format!("tax/round_trip/{n}"), |b| {
            b.iter(|| {
                let v = record_batch_to_json(std::hint::black_box(&batch));
                json_to_record_batch(&v, schema.clone())
            })
        });

        // ── Irreducible work the tax is measured against ──
        // Parquet decode (what the source pays regardless of interchange type).
        c.bench_function(&format!("work/parquet_decode/{n}"), |b| {
            b.iter_batched(
                || encoded.clone(),
                |bytes| parquet_decode(std::hint::black_box(bytes)),
                BatchSize::SmallInput,
            )
        });
        // Parquet encode (what the sink pays regardless of interchange type).
        c.bench_function(&format!("work/parquet_encode/{n}"), |b| {
            b.iter(|| parquet_encode(std::hint::black_box(&batch)))
        });

        // ── S3-bulk variant: JSONL text (de)serialization ──
        // The tax an S3 bulk object pays today. Compare its round-trip against
        // `work/parquet_*` above: that difference is the saving an Arrow-native
        // Parquet-on-S3 path would capture for bulk analytical objects.
        let jsonl = s3_jsonl_encode(&recs);
        c.bench_function(&format!("s3_bulk/jsonl_encode/{n}"), |b| {
            b.iter(|| s3_jsonl_encode(std::hint::black_box(&recs)))
        });
        c.bench_function(&format!("s3_bulk/jsonl_decode/{n}"), |b| {
            b.iter(|| s3_jsonl_decode(std::hint::black_box(&jsonl)))
        });
        c.bench_function(&format!("s3_bulk/jsonl_round_trip/{n}"), |b| {
            b.iter(|| s3_jsonl_decode(&s3_jsonl_encode(std::hint::black_box(&recs))))
        });
        // DuckDB SQL over the page — the transform's real work. `apply_stages_to_page`
        // includes the crate's own internal Value↔Arrow conversions, so this is the
        // full transform-boundary cost as shipped today.
        //
        // NOTE: capped at ≤1000 rows. Feeding a single page larger than DuckDB's
        // standard vector size through the `vtab-arrow` bridge aborts the process
        // (`assertion failed: array.len() <= out.capacity()` in duckdb-rs) — a real
        // large-page defect tracked separately, not something this benchmark should
        // exercise. The tax/parquet primitives above run at every size.
        if n <= 1_000 {
            let agg = sql_stage(
                "SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM batch GROUP BY region",
            );
            c.bench_function(&format!("work/duckdb_groupby/{n}"), |b| {
                b.iter(|| apply_stages_to_page(recs.clone(), std::slice::from_ref(&agg)).unwrap())
            });
        }
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
