//! Compares json_schema validation vs the equivalent granular checks per record.

use criterion::{Criterion, criterion_group, criterion_main};
use faucet_core::quality::{CompiledQuality, apply_quality};
use faucet_core::{OnFailure, QualitySpec, RecordCheck};
use serde_json::json;
use std::hint::black_box;

fn bench_quality(c: &mut Criterion) {
    let records: Vec<_> = (0..1000)
        .map(|i| json!({"id": i, "age": 30, "email": "a@b.com"}))
        .collect();

    let granular = CompiledQuality::compile(&QualitySpec {
        record: vec![
            RecordCheck::NotNull { field: "id".into(), treat_missing_as_null: true, on_failure: OnFailure::Abort },
            RecordCheck::Compare { field: "age".into(), op: faucet_core::CompareOp::Gte, value: json!(0), on_failure: OnFailure::Abort },
        ],
        batch: vec![],
    })
    .unwrap();

    c.bench_function("quality_granular_1k", |b| {
        b.iter(|| apply_quality(black_box(records.clone()), &granular).unwrap())
    });

    let schema = CompiledQuality::compile(&QualitySpec {
        record: vec![RecordCheck::JsonSchema {
            schema: json!({"type":"object","required":["id"],"properties":{"id":{"type":"integer"},"age":{"type":"integer","minimum":0}}}),
            on_failure: OnFailure::Abort,
        }],
        batch: vec![],
    })
    .unwrap();

    c.bench_function("quality_json_schema_1k", |b| {
        b.iter(|| apply_quality(black_box(records.clone()), &schema).unwrap())
    });
}

criterion_group!(benches, bench_quality);
criterion_main!(benches);
