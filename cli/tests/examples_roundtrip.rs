//! Verify every shipped example YAML loads and expands cleanly.
//!
//! Guards against accidental schema-changes (named-templates feature, etc.)
//! breaking real-world configs. Any example that fails this test is a
//! backwards-compatibility regression.
//!
//! Examples that reference `${env:VAR}` or `${file:PATH}` placeholders
//! require credentials that are not available in test environments. Those
//! load errors are treated as "skipped" — the test only fails on structural
//! errors (bad YAML, unknown fields, expand failures) that indicate a
//! backwards-compatibility regression.

use faucet_cli::config::PipelineConfig;
use faucet_cli::error::CliError;
use faucet_cli::expand::expand;
use std::path::PathBuf;

fn examples_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples")
}

/// Returns `true` when the error is a missing credential (env var / file) —
/// these are expected in environments without secrets and should be skipped.
fn is_credential_error(e: &CliError) -> bool {
    matches!(
        e,
        CliError::MissingEnvVar { .. } | CliError::ReadInterpolatedFile { .. }
    )
}

#[test]
fn every_example_loads_and_expands() {
    let dir = examples_dir();
    let mut count = 0;
    let mut skipped = 0;
    let mut failures: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("examples dir exists") {
        let path = entry.expect("readable dir entry").path();
        let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
            continue;
        };
        if !matches!(ext, "yaml" | "yml" | "json") {
            continue;
        }
        count += 1;
        let cfg = match PipelineConfig::from_path(&path) {
            Ok(c) => c,
            Err(ref e) if is_credential_error(e) => {
                // Credential not available in this environment — skip.
                skipped += 1;
                continue;
            }
            Err(e) => {
                failures.push(format!("load {}: {e}", path.display()));
                continue;
            }
        };
        if let Err(e) = expand(&cfg) {
            failures.push(format!("expand {}: {e}", path.display()));
        }
    }
    assert!(count > 10, "expected to find >10 examples, found {count}");
    eprintln!("examples: {count} total, {skipped} skipped (missing credentials), {} checked", count - skipped);
    assert!(
        failures.is_empty(),
        "{} example(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
