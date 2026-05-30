//! `${now.*}` interpolation end-to-end via `faucet run --clock`.
use assert_cmd::Command;

#[test]
fn now_date_resolves_in_sink_path_with_clock_override() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "name\nalice\n").unwrap();
    // Sink path templated with ${now.date}; --clock makes it deterministic.
    // Note: the jsonl sink does NOT create missing parent directories, so we
    // use a flat path (no subdirectory) to avoid an I/O error.
    let cfg = dir.path().join("pipeline.yaml");
    std::fs::write(
        &cfg,
        format!(
            "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: \"{csv}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{out}\" }} }}\n",
            csv = csv.display(),
            out = dir.path().join("out-${now.date}.jsonl").display(),
        ),
    )
    .unwrap();
    Command::cargo_bin("faucet")
        .unwrap()
        .arg("run")
        .arg(&cfg)
        .arg("--clock")
        .arg("2026-03-08")
        .assert()
        .success();
    let expected = dir.path().join("out-2026-03-08.jsonl");
    assert!(
        expected.exists(),
        "expected dated sink path {expected:?} to be created"
    );
}
