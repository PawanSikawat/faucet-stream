//! End-to-end tests for the runtime matrix-row selection model
//! (#370 identity, #371 status, #376 tags, #377 include_parents), driving the
//! real `faucet run` binary and asserting which rows' sink outputs are produced.

#![cfg(all(feature = "source-csv", feature = "sink-jsonl"))]

use assert_cmd::Command;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

/// Write the input CSV + a HiBob-style multi-endpoint config whose rows each
/// write to a distinct absolute JSONL path under `dir`. Returns the config
/// path plus the three per-row output paths.
fn scaffold(dir: &Path) -> (std::path::PathBuf, [std::path::PathBuf; 3]) {
    let input = dir.join("in.csv");
    fs::write(&input, "name\nalice\nbob\n").unwrap();

    let people = dir.join("people.jsonl");
    let audit = dir.join("audit.jsonl");
    let payroll = dir.join("payroll.jsonl");

    let cfg = dir.join("pipeline.yaml");
    let yaml = format!(
        r#"version: 1
name: sel
pipeline:
  sources:
    src:
      type: csv
      config: {{ path: {input} }}
  sinks:
    out:
      type: jsonl
      config: {{ append: false }}
matrix:
  - id: people
    source: {{ ref: src, status: active }}
    sink: {{ ref: out, config: {{ path: {people} }} }}
    tags: [core, daily]
  - id: audit
    source: {{ ref: src, status: available }}
    sink: {{ ref: out, config: {{ path: {audit} }} }}
    tags: [finance]
  - id: payroll
    source: {{ ref: src, status: mandatory }}
    sink: {{ ref: out, config: {{ path: {payroll} }} }}
    tags: [finance]
"#,
        input = input.display(),
        people = people.display(),
        audit = audit.display(),
        payroll = payroll.display(),
    );
    fs::write(&cfg, yaml).unwrap();
    (cfg, [people, audit, payroll])
}

fn run<'a>(cfg: &Path, extra: impl IntoIterator<Item = &'a str>) -> assert_cmd::assert::Assert {
    let mut cmd = Command::cargo_bin("faucet").unwrap();
    cmd.arg("run").arg(cfg);
    for a in extra {
        cmd.arg(a);
    }
    cmd.assert()
}

#[test]
fn bare_run_executes_mandatory_and_active_only() {
    let dir = TempDir::new().unwrap();
    let (cfg, [people, audit, payroll]) = scaffold(dir.path());
    run(&cfg, []).success();
    assert!(people.exists(), "active `people` should run");
    assert!(payroll.exists(), "mandatory `payroll` should run");
    assert!(
        !audit.exists(),
        "available `audit` must be gated out by default"
    );
}

#[test]
fn status_and_tag_compose() {
    let dir = TempDir::new().unwrap();
    let (cfg, [people, audit, payroll]) = scaffold(dir.path());
    // Raise the gate to `available` and narrow to the `finance` domain:
    // payroll (mandatory+finance) and audit (available+finance) run; people
    // (active but not finance) does not.
    run(&cfg, ["--status", "available", "--tag", "finance"]).success();
    assert!(payroll.exists(), "mandatory finance row runs");
    assert!(
        audit.exists(),
        "available finance row runs once --status available"
    );
    assert!(!people.exists(), "non-finance row narrowed out by --tag");
}

#[test]
fn select_by_id_forces_a_parked_row() {
    let dir = TempDir::new().unwrap();
    let (cfg, [people, audit, payroll]) = scaffold(dir.path());
    // `audit` is `available` (parked) but explicitly selected by id → runs;
    // nothing else does.
    run(&cfg, ["--select", "audit"]).success();
    assert!(
        audit.exists(),
        "explicitly selected row runs regardless of status"
    );
    assert!(!people.exists());
    assert!(!payroll.exists());
}

#[test]
fn unknown_select_token_is_a_hard_error() {
    let dir = TempDir::new().unwrap();
    let (cfg, _) = scaffold(dir.path());
    run(&cfg, ["--select", "peeple"])
        .failure()
        .stderr(predicates::str::contains("matched no matrix row"));
}

#[test]
fn env_var_selection_is_honored() {
    let dir = TempDir::new().unwrap();
    let (cfg, [people, audit, payroll]) = scaffold(dir.path());
    let mut cmd = Command::cargo_bin("faucet").unwrap();
    cmd.env("FAUCET_SELECT", "people")
        .arg("run")
        .arg(&cfg)
        .assert()
        .success();
    assert!(people.exists());
    assert!(!audit.exists());
    assert!(!payroll.exists());
}
