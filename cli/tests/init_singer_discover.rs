//! `faucet init --source singer --discover` end-to-end (drives the command
//! function directly against the dependency-free fake tap — no binary spawn).

#![cfg(feature = "source-singer")]

use std::path::PathBuf;

use faucet_cli::cli::InitArgs;

fn fake_tap() -> String {
    format!(
        "{}/../crates/source/singer/tests/fake_taps/fake_tap.sh",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn init_args(output: PathBuf, executable: Option<String>, discover: bool) -> InitArgs {
    InitArgs {
        name: Some("t".into()),
        source: Some("singer".into()),
        sink: Some("jsonl".into()),
        output,
        force: true,
        interactive: false,
        template: "default".into(),
        discover,
        executable,
    }
}

#[tokio::test]
async fn discover_writes_catalog_and_scaffold() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("pipeline.yaml");
    let args = init_args(out.clone(), Some(fake_tap()), true);

    faucet_cli::commands::init::run(args)
        .await
        .expect("init --discover");

    let yaml = std::fs::read_to_string(&out).unwrap();
    // Lists discovered streams and leaves stream unset.
    assert!(
        yaml.contains("Discovered streams: s, audit_log"),
        "yaml:\n{yaml}"
    );
    assert!(yaml.contains("stream: \"\""));
    // Catalog inlined as a JSON object (not a `${file:}` string).
    assert!(yaml.contains("\"streams\""));
    // Catalog also written to disk.
    let catalog = dir.path().join("catalog.json");
    assert!(catalog.exists(), "catalog.json should be written");
    let cat: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(catalog).unwrap()).unwrap();
    assert!(cat.get("streams").is_some());
}

#[tokio::test]
async fn discover_requires_executable() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("pipeline.yaml");
    let args = init_args(out, None, true);
    let err = faucet_cli::commands::init::run(args)
        .await
        .expect_err("must fail without --executable");
    assert!(err.to_string().contains("--executable"), "got: {err}");
}
