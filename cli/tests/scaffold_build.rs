//! Proves a scaffolded connector crate actually compiles and tests green
//! against the published `faucet-core` (#209 — "generated crate builds green").
//!
//! `#[ignore]`d because it shells out to a nested `cargo test` that downloads
//! `faucet-core` from crates.io and does a full compile — too slow/network-bound
//! for the default suite. Run explicitly (locally or in a dedicated CI step):
//!
//! ```console
//! cargo test -p faucet-cli --test scaffold_build -- --ignored
//! ```
//!
//! The pure template conventions (docs.rs metadata, `cfg_attr` crate-root line,
//! `JsonSchema` derive, system-name-first keywords, `version = "1.0.0"`, trait
//! impls) are asserted cheaply and always by the unit tests in
//! `faucet_cli::scaffold`.

use faucet_cli::scaffold::{ConnectorKind, ConnectorScaffold};
use std::process::Command;

fn scaffold_to(dir: &std::path::Path, kind: ConnectorKind, common: bool) -> std::path::PathBuf {
    let s = ConnectorScaffold::new("acme", kind, common).unwrap();
    for f in s.files() {
        let path = dir.join(&f.path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &f.contents).unwrap();
    }
    dir.join(s.crate_name())
}

fn cargo_test(manifest: &std::path::Path) {
    let status = Command::new(env!("CARGO"))
        .args(["test", "--manifest-path"])
        .arg(manifest.join("Cargo.toml"))
        .status()
        .expect("run nested cargo test");
    assert!(status.success(), "generated crate failed to build/test");
}

#[test]
#[ignore = "nested cargo build; run with --ignored"]
fn generated_source_crate_builds_and_tests() {
    let dir = tempfile::tempdir().unwrap();
    let crate_dir = scaffold_to(dir.path(), ConnectorKind::Source, false);
    cargo_test(&crate_dir);
}

#[test]
#[ignore = "nested cargo build; run with --ignored"]
fn generated_sink_crate_builds_and_tests() {
    let dir = tempfile::tempdir().unwrap();
    let crate_dir = scaffold_to(dir.path(), ConnectorKind::Sink, false);
    cargo_test(&crate_dir);
}
