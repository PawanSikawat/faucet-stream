//! Integration tests for the `faucet` binary. Each test drives the binary
//! built by cargo via `assert_cmd`, mirroring how users invoke it.

use assert_cmd::Command;
use predicates::str::contains;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

/// Helper: a config that reads two records from a CSV file and writes them
/// as JSONL. Uses absolute paths so the test isn't sensitive to cwd.
fn csv_to_jsonl_yaml(csv: &Path, out: &Path) -> String {
    format!(
        r#"version: 1
name: csv_to_jsonl_smoke
source:
  type: csv
  config:
    path: {csv}
sink:
  type: jsonl
  config:
    path: {out}
"#,
        csv = csv.display(),
        out = out.display(),
    )
}

#[test]
fn list_lists_compiled_in_connectors() {
    Command::cargo_bin("faucet")
        .unwrap()
        .arg("list")
        .assert()
        .success()
        .stdout(contains("Sources:"))
        .stdout(contains("Sinks:"))
        .stdout(contains("rest "))
        .stdout(contains("jsonl "));
}

#[test]
fn schema_prints_jsonl_sink_schema() {
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["schema", "sink", "jsonl"])
        .assert()
        .success()
        .stdout(contains("\"path\""));
}

#[test]
fn schema_rejects_unknown_kind() {
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["schema", "source", "nope"])
        .assert()
        .failure()
        .stderr(contains("unknown source 'nope'"));
}

#[test]
fn init_scaffolds_pipeline_yaml() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "my_pipeline", "--output"])
        .arg(&out)
        .assert()
        .success()
        .stdout(contains("wrote"));
    let body = fs::read_to_string(&out).unwrap();
    assert!(body.contains("name: my_pipeline"));
    assert!(body.contains("type: rest"));
}

#[test]
fn init_refuses_to_overwrite_existing_file() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    fs::write(&out, "version: 1\n").unwrap();
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "again", "--output"])
        .arg(&out)
        .assert()
        .failure()
        .stderr(contains("refusing to overwrite"));
}

#[test]
fn validate_accepts_csv_to_jsonl_yaml() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name,score\nalice,1\nbob,2\n").unwrap();

    let yaml = csv_to_jsonl_yaml(&csv, &out);
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, yaml).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("source=csv"))
        .stdout(contains("sink=jsonl"));
}

#[test]
fn run_executes_csv_to_jsonl_pipeline() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name,score\nalice,1\nbob,2\n").unwrap();

    let yaml = csv_to_jsonl_yaml(&csv, &out);
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, yaml).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("wrote 2 records"));

    let lines: Vec<_> = fs::read_to_string(&out)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("\"alice\""));
}

#[test]
fn run_with_dry_run_does_not_touch_the_sink_path() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name\nalice\nbob\n").unwrap();

    let yaml = csv_to_jsonl_yaml(&csv, &out);
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, yaml).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", "--dry-run"])
        .arg(&cfg)
        .assert()
        .success();

    assert!(
        !out.exists(),
        "dry-run must not write to the configured sink path"
    );
}

#[test]
fn run_with_limit_caps_records_written() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name\nalice\nbob\ncarol\n").unwrap();

    let yaml = csv_to_jsonl_yaml(&csv, &out);
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, yaml).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", "--limit", "2"])
        .arg(&cfg)
        .assert()
        .success();

    let body = fs::read_to_string(&out).unwrap();
    assert_eq!(body.lines().count(), 2);
}

#[test]
fn run_with_state_path_persists_a_bookmark_dir() {
    // CSV source doesn't return bookmarks; this just exercises the wiring so
    // the override doesn't crash when present alongside a non-stateful source.
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name\nalice\n").unwrap();
    let yaml = csv_to_jsonl_yaml(&csv, &out);
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, yaml).unwrap();
    let state_dir = dir.path().join("state");

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", "--state-path"])
        .arg(&state_dir)
        .arg(&cfg)
        .assert()
        .success();
}

#[test]
fn env_interpolation_resolves_inside_config_values() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name\nalice\n").unwrap();

    let cfg_text = format!(
        r#"version: 1
source:
  type: csv
  config:
    path: ${{env:FAUCET_TEST_CSV_PATH}}
sink:
  type: jsonl
  config:
    path: {out}
"#,
        out = out.display()
    );
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, cfg_text).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .env("FAUCET_TEST_CSV_PATH", &csv)
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success();
}

#[test]
fn shipped_example_yamls_pass_validate() {
    // Validate every example under cli/examples/. The YAMLs reference
    // environment variables that the env-interpolator needs present, so
    // stuff placeholders in for every var any example mentions. `validate`
    // is offline — placeholders are safe.
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let env_placeholders: &[(&str, &str)] = &[
        ("API_KEY", "x"),
        ("API_TOKEN", "x"),
        ("API_USER", "x"),
        ("API_PASS", "x"),
        ("AUTH_TOKEN", "x"),
        ("ES_USER", "x"),
        ("ES_PASS", "x"),
        ("ES_API_KEY", "x"),
        ("GCP_KEY_JSON", "{}"),
        ("GITHUB_TOKEN", "x"),
        ("GRPC_API_KEY", "x"),
        ("GRPC_TOKEN", "x"),
        ("INGEST_TOKEN", "x"),
        ("INGEST_USER", "x"),
        ("INGEST_PASS", "x"),
        ("PG_URL", "postgres://u:p@localhost/db"),
        ("SNOWFLAKE_OAUTH_TOKEN", "x"),
        ("SOAP_USER", "x"),
        ("SOAP_PASS", "x"),
        ("STRIPE_TOKEN", "x"),
        ("FEED_TOKEN", "x"),
        (
            "GOOGLE_APPLICATION_CREDENTIALS",
            "/tmp/service-account.json",
        ),
    ];
    let examples_dir = std::path::Path::new(manifest_dir).join("examples");
    // Some examples interpolate `${file:./snowflake_key.pem}`. We run faucet
    // with cwd set to a temp dir that holds a placeholder PEM so the file
    // directive can resolve.
    let workdir = TempDir::new().unwrap();
    fs::write(workdir.path().join("snowflake_key.pem"), "dummy-key").unwrap();

    let mut count = 0;
    for entry in fs::read_dir(&examples_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        count += 1;
        let mut cmd = Command::cargo_bin("faucet").unwrap();
        for (k, v) in env_placeholders {
            cmd.env(k, v);
        }
        cmd.current_dir(workdir.path())
            .args(["validate"])
            .arg(&path)
            .assert()
            .success();
    }
    assert!(count >= 30, "expected many YAML examples, got {count}");
}

#[test]
fn missing_env_var_in_config_is_reported() {
    let dir = TempDir::new().unwrap();
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(
        &cfg,
        r#"version: 1
source:
  type: csv
  config:
    path: ${env:FAUCET_DEFINITELY_UNSET}
sink:
  type: jsonl
  config:
    path: /tmp/no.jsonl
"#,
    )
    .unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .env_remove("FAUCET_DEFINITELY_UNSET")
        .args(["validate"])
        .arg(&cfg)
        .assert()
        .failure()
        .stderr(contains("missing environment variable"));
}
