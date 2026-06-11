//! End-to-end: a base + overlay + `--profile` composes correctly through the
//! `faucet` binary.

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;

fn faucet() -> Command {
    Command::cargo_bin("faucet").expect("faucet binary builds")
}

#[test]
fn run_with_profile_selects_overlay_sink() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("in.csv");
    fs::write(&data, "id,name\n1,alice\n2,bob\n").unwrap();

    fs::write(
        dir.path().join("base.yaml"),
        format!(
            "version: 1\nname: composed\npipeline:\n  source: {{ type: csv, config: {{ path: {csv} }} }}\n  sink: {{ type: jsonl, config: {{ path: {dev} }} }}\nprofiles:\n  prod:\n    pipeline:\n      sink: {{ config: {{ path: {prod} }} }}\n",
            csv = data.display(),
            dev = dir.path().join("dev.jsonl").display(),
            prod = dir.path().join("prod.jsonl").display(),
        ),
    )
    .unwrap();
    let app = dir.path().join("app.yaml");
    fs::write(&app, "extends: ./base.yaml\n").unwrap();

    faucet()
        .args(["run", app.to_str().unwrap(), "--profile", "prod"])
        .assert()
        .success();

    assert!(dir.path().join("prod.jsonl").exists(), "prod overlay sink must be written");
    assert!(!dir.path().join("dev.jsonl").exists(), "base sink must NOT be written when prod selected");
}

#[test]
fn env_profile_is_honored_and_flag_overrides_it() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("in.csv");
    fs::write(&data, "id\n1\n").unwrap();
    fs::write(
        dir.path().join("p.yaml"),
        format!(
            "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: {csv} }} }}\n  sink: {{ type: jsonl, config: {{ path: {base} }} }}\nprofiles:\n  dev: {{ pipeline: {{ sink: {{ config: {{ path: {dev} }} }} }} }}\n  prod: {{ pipeline: {{ sink: {{ config: {{ path: {prod} }} }} }} }}\n",
            csv = data.display(),
            base = dir.path().join("base.jsonl").display(),
            dev = dir.path().join("dev.jsonl").display(),
            prod = dir.path().join("prod.jsonl").display(),
        ),
    )
    .unwrap();
    let p = dir.path().join("p.yaml");

    // FAUCET_PROFILE=dev (no flag) → dev.jsonl
    faucet().args(["run", p.to_str().unwrap()]).env("FAUCET_PROFILE", "dev").assert().success();
    assert!(dir.path().join("dev.jsonl").exists());

    // --profile prod overrides FAUCET_PROFILE=dev → prod.jsonl, NOT dev.jsonl.
    // Remove dev.jsonl first so its non-recreation proves the env var was suppressed.
    fs::remove_file(dir.path().join("dev.jsonl")).unwrap();
    faucet()
        .args(["run", p.to_str().unwrap(), "--profile", "prod"])
        .env("FAUCET_PROFILE", "dev")
        .assert()
        .success();
    assert!(dir.path().join("prod.jsonl").exists(), "prod overlay must be written");
    assert!(
        !dir.path().join("dev.jsonl").exists(),
        "dev sink must NOT be written — --profile prod overrides FAUCET_PROFILE=dev"
    );
}

#[test]
fn show_composed_prints_merged_config_with_profile_applied() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("base.yaml"),
        "version: 1\npipeline:\n  source: { type: csv, config: { path: x.csv } }\n  sink: { type: jsonl, config: { path: base.jsonl } }\nprofiles:\n  prod:\n    pipeline:\n      sink: { config: { path: prod.jsonl } }\n",
    )
    .unwrap();
    let app = dir.path().join("app.yaml");
    fs::write(&app, "extends: ./base.yaml\n").unwrap();

    faucet()
        .args(["validate", app.to_str().unwrap(), "--profile", "prod", "--show-composed", "--no-secrets"])
        .assert()
        .success()
        .stdout(predicates::str::contains("prod.jsonl"))      // profile applied
        .stdout(predicates::str::contains("base.jsonl").not()) // base sink overridden
        .stdout(predicates::str::contains("profiles:").not())  // metadata stripped
        .stdout(predicates::str::contains("extends:").not());
}

#[test]
fn unknown_profile_fails_clearly() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("p.yaml"),
        "version: 1\npipeline:\n  source: { type: csv, config: { path: x.csv } }\n  sink: { type: jsonl, config: { path: o.jsonl } }\nprofiles:\n  dev: {}\n",
    )
    .unwrap();
    faucet()
        .args(["validate", dir.path().join("p.yaml").to_str().unwrap(), "--profile", "staging", "--no-secrets"])
        .assert()
        .failure()
        .stderr(predicates::str::contains("unknown profile 'staging'"));
}
