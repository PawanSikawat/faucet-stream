//! End-to-end tests for `faucet schedule`. Gated on the feature so non-schedule
//! builds skip them.
#![cfg(feature = "schedule")]

use assert_cmd::Command;
use std::time::Duration;

/// Write a CSV + a schedule config into a temp dir; return (dir, config path).
fn fixture(schedule_block: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "name\nalice\nbob\n").unwrap();
    let out = dir.path().join("out.jsonl");
    let cfg = dir.path().join("pipeline.yaml");
    std::fs::write(
        &cfg,
        format!(
            r#"version: 1
name: sched-test
schedule:
{schedule_block}
pipeline:
  source: {{ type: csv, config: {{ path: {csv} }} }}
  sink:   {{ type: jsonl, config: {{ path: {out} }} }}
"#,
            csv = csv.display(),
            out = out.display(),
        ),
    )
    .unwrap();
    (dir, cfg)
}

#[test]
fn once_runs_one_pipeline_and_exits_zero() {
    let (dir, cfg) = fixture("  cron: \"0 2 * * *\"\n");
    Command::cargo_bin("faucet")
        .unwrap()
        .arg("schedule")
        .arg(&cfg)
        .arg("--once")
        .assert()
        .success();
    let out = std::fs::read_to_string(dir.path().join("out.jsonl")).unwrap();
    assert_eq!(
        out.lines().count(),
        2,
        "expected the two CSV rows written once"
    );
}

#[test]
fn max_runs_stops_the_loop() {
    // Seconds-cron firing every second; stop after 2 successful runs.
    let (dir, cfg) =
        fixture("  cron: \"*/1 * * * * *\"\n  max_runs: 2\n  start_immediately: true\n");
    Command::cargo_bin("faucet")
        .unwrap()
        .arg("schedule")
        .arg(&cfg)
        .timeout(Duration::from_secs(20))
        .assert()
        .success();
    // The loop terminated on its own (max_runs) — the output file exists.
    assert!(dir.path().join("out.jsonl").exists());
}

#[test]
fn missing_schedule_block_is_a_clear_error() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "name\nx\n").unwrap();
    let cfg = dir.path().join("pipeline.yaml");
    std::fs::write(
        &cfg,
        format!(
            "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: {csv} }} }}\n  sink: {{ type: stdout, config: {{}} }}\n",
            csv = csv.display()
        ),
    )
    .unwrap();
    Command::cargo_bin("faucet")
        .unwrap()
        .arg("schedule")
        .arg(&cfg)
        .arg("--once")
        .assert()
        .failure()
        .stderr(predicates::str::contains("schedule"));
}

#[test]
fn validate_rejects_bad_schedule_cron() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "name\nx\n").unwrap();
    let cfg = dir.path().join("pipeline.yaml");
    std::fs::write(
        &cfg,
        format!(
            "version: 1\nschedule:\n  cron: \"not a cron\"\npipeline:\n  source: {{ type: csv, config: {{ path: {csv} }} }}\n  sink: {{ type: jsonl, config: {{ path: {out} }} }}\n",
            csv = csv.display(),
            out = dir.path().join("o.jsonl").display(),
        ),
    )
    .unwrap();
    assert_cmd::Command::cargo_bin("faucet")
        .unwrap()
        .arg("validate")
        .arg(&cfg)
        .assert()
        .failure()
        .stderr(predicates::str::contains("cron"));
}
