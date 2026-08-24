//! `${parent.*}` / `${now.*}` tokens in a `set` transform value, resolved per
//! record like source/sink configs (#568). A child fan-out stamps its parent's
//! key as a column via `set`, and a `${now.*}` set value resolves per run.
use assert_cmd::Command;

#[test]
fn set_stamps_parent_key_and_now_per_child_invocation() {
    let dir = tempfile::tempdir().unwrap();
    // Parent produces two records with distinct ids.
    let parents = dir.path().join("parents.csv");
    std::fs::write(&parents, "id\n1\n2\n").unwrap();
    // Child source yields a single row per parent invocation.
    let child = dir.path().join("child.csv");
    std::fs::write(&child, "v\nx\n").unwrap();

    let parents_out = dir.path().join("parents-out.jsonl");
    let cfg = dir.path().join("pipeline.yaml");
    std::fs::write(
        &cfg,
        format!(
            "version: 1\n\
             name: set_parent_token\n\
             pipeline:\n\
             \x20 source: {{ type: csv, config: {{ path: \"{parents}\" }} }}\n\
             \x20 sink: {{ type: jsonl, config: {{ path: \"{parents_out}\" }} }}\n\
             matrix:\n\
             \x20 - id: p\n\
             \x20   source: {{ config: {{ path: \"{parents}\" }} }}\n\
             \x20   sink: {{ config: {{ path: \"{parents_out}\" }} }}\n\
             \x20 - id: c\n\
             \x20   parent: p\n\
             \x20   source: {{ config: {{ path: \"{child}\" }} }}\n\
             \x20   sink: {{ config: {{ path: \"{dir}/child-${{p.id}}.jsonl\" }} }}\n\
             \x20   transforms:\n\
             \x20     - {{ type: set, config: {{ values: {{ pid: \"${{p.id}}\", run_date: \"${{now.date}}\" }} }} }}\n",
            parents = parents.display(),
            parents_out = parents_out.display(),
            child = child.display(),
            dir = dir.path().display(),
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

    for id in ["1", "2"] {
        let out = dir.path().join(format!("child-{id}.jsonl"));
        assert!(out.exists(), "expected child output {out:?}");
        let body = std::fs::read_to_string(&out).unwrap();
        let row: serde_json::Value = serde_json::from_str(body.lines().next().unwrap()).unwrap();
        // Parent key stamped as a column, resolved per child invocation.
        assert_eq!(row["pid"], serde_json::json!(id), "pid in {out:?}: {body}");
        // `${now.*}` in the same `set` still resolves.
        assert_eq!(row["run_date"], serde_json::json!("2026-03-08"));
        // The child's own field survives.
        assert_eq!(row["v"], serde_json::json!("x"));
    }
}
