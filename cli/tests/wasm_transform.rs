//! End-to-end: a real `faucet run` pipeline with a WASM transform, driving the
//! precompiled reference module at `examples/wasm-transforms/add_field.wasm`
//! (built from `examples/wasm-transforms/rust/`). Gated on `transform-wasm`.
#![cfg(feature = "transform-wasm")]

use assert_cmd::Command;
use std::fs;
use tempfile::TempDir;

/// The committed reference module (Rust → wasm): uppercases `name` and stamps
/// `wasm_processed: true`.
const ADD_FIELD_WASM: &[u8] = include_bytes!("../../examples/wasm-transforms/add_field.wasm");

#[test]
fn run_pipeline_with_wasm_transform() {
    let dir = TempDir::new().unwrap();
    let module = dir.path().join("add_field.wasm");
    fs::write(&module, ADD_FIELD_WASM).unwrap();

    let csv = dir.path().join("in.csv");
    fs::write(&csv, "name,id\nalice,1\nbob,2\n").unwrap();
    let out = dir.path().join("out.jsonl");

    let cfg = dir.path().join("pipeline.yaml");
    fs::write(
        &cfg,
        format!(
            r#"version: 1
name: wasm_e2e
pipeline:
  source: {{ type: csv, config: {{ path: {csv} }} }}
  transforms:
    - type: wasm
      config:
        module: {module}
  sink: {{ type: jsonl, config: {{ path: {out} }} }}
"#,
            csv = csv.display(),
            module = module.display(),
            out = out.display(),
        ),
    )
    .unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success();

    let written = fs::read_to_string(&out).unwrap();
    let lines: Vec<serde_json::Value> = written
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2, "two records expected, got: {written}");
    // The module uppercases `name` and adds `wasm_processed: true`.
    assert_eq!(lines[0]["name"], "ALICE");
    assert_eq!(lines[0]["wasm_processed"], true);
    assert_eq!(lines[1]["name"], "BOB");
    assert_eq!(lines[1]["wasm_processed"], true);
}

#[test]
fn run_fails_when_module_missing() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    fs::write(&csv, "name,id\nalice,1\n").unwrap();
    let out = dir.path().join("out.jsonl");
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(
        &cfg,
        format!(
            r#"version: 1
name: wasm_missing
pipeline:
  source: {{ type: csv, config: {{ path: {csv} }} }}
  transforms:
    - type: wasm
      config:
        module: {dir}/does-not-exist.wasm
  sink: {{ type: jsonl, config: {{ path: {out} }} }}
"#,
            csv = csv.display(),
            dir = dir.path().display(),
            out = out.display(),
        ),
    )
    .unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .failure();
}
