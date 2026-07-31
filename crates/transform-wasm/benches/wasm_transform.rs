//! Per-record overhead of the WASM transform on a trivial (identity) module.
//!
//! Run with: `cargo bench -p faucet-transform-wasm`

use criterion::{Criterion, criterion_group, criterion_main};
use faucet_core::stage::{CompiledStage, apply_stages_to_page, compile_stage};
use faucet_transform_wasm::{WasmOnError, WasmTransform, WasmTransformConfig};
use serde_json::{Value, json};
use std::hint::black_box;
use std::io::Write;

/// A minimal identity module: echoes the input record back verbatim.
const IDENTITY_WAT: &str = r#"(module
    (memory (export "memory") 1)
    (global $bump (mut i32) (i32.const 1024))
    (func (export "alloc") (param $len i32) (result i32)
        (local $p i32)
        (local.set $p (global.get $bump))
        (global.set $bump (i32.add (global.get $bump) (local.get $len)))
        (local.get $p))
    (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
        (local $out i32)
        (local.set $out (global.get $bump))
        (global.set $bump (i32.add (global.get $bump) (local.get $len)))
        (memory.copy (local.get $out) (local.get $ptr) (local.get $len))
        (i64.or
            (i64.shl (i64.extend_i32_u (local.get $out)) (i64.const 32))
            (i64.extend_i32_u (local.get $len)))))"#;

fn compiled_identity_stage() -> (CompiledStage, tempfile::NamedTempFile) {
    let wasm = wat::parse_str(IDENTITY_WAT).unwrap();
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(&wasm).unwrap();
    f.flush().unwrap();
    let cfg = WasmTransformConfig {
        module: f.path().to_string_lossy().into_owned(),
        function: "transform".into(),
        memory_limit_mb: 16,
        fuel_limit: 10_000_000,
        on_error: WasmOnError::Fail,
        reload_on_change: false,
    };
    let stage = WasmTransform::compile(&cfg).unwrap().into_page_stage();
    (compile_stage(&stage).unwrap(), f)
}

fn bench_identity(c: &mut Criterion) {
    let (stage, _f) = compiled_identity_stage();
    let stages = [stage];
    let page: Vec<Value> = (0..1000)
        .map(|i| json!({"id": i, "name": "record", "active": true}))
        .collect();

    c.bench_function("wasm_identity_1000_records", |b| {
        b.iter(|| {
            let out = apply_stages_to_page(black_box(page.clone()), &stages).unwrap();
            black_box(out);
        });
    });
}

criterion_group!(benches, bench_identity);
criterion_main!(benches);
