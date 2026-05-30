//! `faucet validate --no-secrets` validates a config that references a secret
//! without needing any backend.

use assert_cmd::Command;
use std::io::Write;

#[test]
fn validate_no_secrets_passes_offline() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("p.yaml");
    let mut f = std::fs::File::create(&cfg).unwrap();
    write!(
        f,
        r#"
version: 1
pipeline:
  source: {{ type: rest, config: {{ base_url: https://x, auth: {{ type: bearer, config: {{ token: "${{vault:secret/data/app#token}}" }} }} }} }}
  sink:   {{ type: jsonl, config: {{ path: ./o.jsonl }} }}
"#
    )
    .unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate", cfg.to_str().unwrap(), "--no-secrets"])
        .assert()
        .success();
}
