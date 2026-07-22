#![cfg(feature = "lineage")]
//! End-to-end: a CSV→JSONL run with file lineage emits START + COMPLETE, and a
//! run with an unreachable lineage backend still succeeds (lineage never fails
//! a run).

use std::io::Write;
use std::path::PathBuf;

/// `RunArgs` does not derive `Default`, so construct it explicitly with the
/// given config path and the field defaults that match `cli/src/cli.rs`.
fn run_args(config: PathBuf) -> faucet_cli::cli::RunArgs {
    faucet_cli::cli::RunArgs {
        config: Some(config),
        from_env: false,
        env_file: None,
        no_env_file: true,
        dry_run: false,
        limit: None,
        state_path: None,
        clock: None,
        profile: None,
        tui: false,
        output: Default::default(),
        selection: Default::default(),
    }
}

#[tokio::test]
async fn run_emits_start_and_complete_to_file() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    let ol = dir.path().join("lineage.jsonl");
    std::fs::write(&input, "id,name\n1,alice\n2,bob\n").unwrap();
    let cfg_path = dir.path().join("pipeline.yaml");
    let mut f = std::fs::File::create(&cfg_path).unwrap();
    write!(
        f,
        r#"version: 1
name: t
lineage:
  namespace: test
  include_schema_facet: true
  transport: {{ type: file, config: {{ path: {ol} }} }}
pipeline:
  source: {{ type: csv, config: {{ path: {input} }} }}
  sink:   {{ type: jsonl, config: {{ path: {output} }} }}
"#,
        ol = ol.display(),
        input = input.display(),
        output = output.display()
    )
    .unwrap();

    faucet_cli::commands::run::run(run_args(cfg_path))
        .await
        .unwrap();

    let body = std::fs::read_to_string(&ol).unwrap();
    let types: Vec<String> = body
        .lines()
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["eventType"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert!(types.contains(&"START".to_string()));
    assert!(types.contains(&"COMPLETE".to_string()));
}

#[tokio::test]
async fn run_succeeds_even_when_lineage_backend_is_unreachable() {
    // Acceptance criterion: lineage emission NEVER fails a run. Point the HTTP
    // transport at a dead port; the CSV→JSONL run must still succeed.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    std::fs::write(&input, "id\n1\n").unwrap();
    let cfg_path = dir.path().join("pipeline.yaml");
    let mut f = std::fs::File::create(&cfg_path).unwrap();
    write!(
        f,
        r#"version: 1
name: t
lineage:
  namespace: test
  transport: {{ type: http, config: {{ url: "http://127.0.0.1:1/api/v1/lineage", timeout_secs: 1 }} }}
pipeline:
  source: {{ type: csv, config: {{ path: {input} }} }}
  sink:   {{ type: jsonl, config: {{ path: {output} }} }}
"#,
        input = input.display(),
        output = output.display()
    )
    .unwrap();

    faucet_cli::commands::run::run(run_args(cfg_path))
        .await
        .expect("run must succeed despite an unreachable lineage backend");
    assert!(output.exists());
}
