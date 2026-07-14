//! `--tui` behavior outside a terminal (#203): on a non-TTY stdout the flag
//! degrades to a plain run (CI/pipe safety), and a build without `cli-tui`
//! rejects the flag with a clear feature hint. Test-harness stdout is never
//! a TTY, so spawning the binary exercises the fallback path directly.

use assert_cmd::Command;

/// Skip when running under `cargo llvm-cov` — spawned-binary tests measure
/// nothing and slow the instrumented suite (repo convention).
fn under_llvm_cov() -> bool {
    std::env::var_os("CARGO_LLVM_COV").is_some()
}

fn write_fixture(dir: &std::path::Path) -> std::path::PathBuf {
    let csv = dir.join("rows.csv");
    std::fs::write(&csv, "id,name\n1,a\n2,b\n3,c\n").unwrap();
    let out = dir.join("out.jsonl");
    let config = dir.join("pipeline.yaml");
    std::fs::write(
        &config,
        format!(
            "version: 1\nname: tui_fallback\npipeline:\n  source:\n    type: csv\n    config: {{ path: {} }}\n  sink:\n    type: jsonl\n    config: {{ path: {} }}\n",
            csv.display(),
            out.display()
        ),
    )
    .unwrap();
    config
}

#[cfg(feature = "cli-tui")]
#[test]
fn tui_flag_on_non_tty_falls_back_to_a_plain_run() {
    if under_llvm_cov() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let config = write_fixture(dir.path());
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", "--tui"])
        .arg(&config)
        .assert()
        .success();
    let out = std::fs::read_to_string(dir.path().join("out.jsonl")).unwrap();
    assert_eq!(out.lines().count(), 3, "pipeline ran normally: {out}");
}

#[cfg(not(feature = "cli-tui"))]
#[test]
fn tui_flag_without_the_feature_is_a_clear_error() {
    if under_llvm_cov() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let config = write_fixture(dir.path());
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", "--tui"])
        .arg(&config)
        .assert()
        .failure()
        .stderr(predicates::str::contains("cli-tui"));
}
