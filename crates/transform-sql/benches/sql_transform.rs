use criterion::{Criterion, criterion_group, criterion_main};
use faucet_core::stage::{apply_stages_to_page, compile_stage};
use faucet_transform_sql::{SqlTransform, SqlTransformConfig};
use serde_json::{Value, json};

fn make_stage(query: &str) -> faucet_core::stage::CompiledStage {
    let cfg = SqlTransformConfig {
        query: query.into(),
        relations: vec![],
        memory_limit: None,
        threads: Some(1),
    };
    compile_stage(&SqlTransform::compile(&cfg).unwrap().into_page_stage()).unwrap()
}

fn page(n: usize) -> Vec<Value> {
    (0..n)
        .map(|i| json!({"k": i % 8, "v": i as f64}))
        .collect()
}

fn bench(c: &mut Criterion) {
    for &n in &[100usize, 1_000, 10_000] {
        let p = page(n);

        let passthrough = make_stage("SELECT * FROM batch");
        c.bench_function(&format!("passthrough/{n}"), |b| {
            b.iter(|| {
                apply_stages_to_page(p.clone(), std::slice::from_ref(&passthrough)).unwrap()
            })
        });

        let agg = make_stage("SELECT k, SUM(v) AS total FROM batch GROUP BY k");
        c.bench_function(&format!("groupby/{n}"), |b| {
            b.iter(|| apply_stages_to_page(p.clone(), std::slice::from_ref(&agg)).unwrap())
        });
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
