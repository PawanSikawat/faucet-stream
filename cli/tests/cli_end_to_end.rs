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
pipeline:
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
fn init_no_args_uses_rest_jsonl_defaults() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "--output"])
        .arg(&out)
        .assert()
        .success();
    let body = fs::read_to_string(&out).unwrap();
    assert!(body.contains("name: my-pipeline"));
    assert!(body.contains("type: rest"));
    assert!(body.contains("type: jsonl"));
}

#[test]
fn init_with_source_sink_flags_uses_those_kinds() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args([
            "init", "my_pipe", "--source", "rest", "--sink", "bigquery", "-o",
        ])
        .arg(&out)
        .assert()
        .success();
    let body = fs::read_to_string(&out).unwrap();
    assert!(body.contains("name: my_pipe"));
    assert!(body.contains("type: rest"));
    assert!(body.contains("type: bigquery"));
    // Required fields are surfaced with the REQUIRED marker so users know
    // exactly what to fill in.
    assert!(body.contains("# REQUIRED"));
    assert!(body.contains("project_id"));
    assert!(body.contains("dataset_id"));
    // Optional fields are commented out so users don't accidentally override
    // their connector-level defaults.
    assert!(body.contains("# batch_size"));
    // Tagged-enum fields (here: BigQuery `credentials:` and REST `auth:`) emit
    // every variant as a commented "alternative" block so users can switch
    // without bouncing to `faucet schema`. Default variant is inline; the
    // alternatives header announces the rest.
    assert!(
        body.contains("Alternative variants"),
        "missing alternatives block:\n{body}"
    );
    assert!(
        body.contains("# type: Bearer"),
        "REST Bearer alternative missing:\n{body}"
    );
    assert!(
        body.contains("# type: OAuth2"),
        "REST OAuth2 alternative missing:\n{body}"
    );
    assert!(
        body.contains("# type: ApplicationDefault"),
        "BigQuery ApplicationDefault alternative missing:\n{body}"
    );
}

#[test]
fn init_unknown_source_kind_lists_available_kinds() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "--source", "nope", "--output"])
        .arg(&out)
        .assert()
        .failure()
        .stderr(contains("unknown source 'nope'"))
        .stderr(contains("rest"));
}

#[test]
fn init_unknown_sink_kind_lists_available_kinds() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "--sink", "nope", "--output"])
        .arg(&out)
        .assert()
        .failure()
        .stderr(contains("unknown sink 'nope'"))
        .stderr(contains("jsonl"));
}

#[test]
fn init_force_overwrites_existing_file() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    fs::write(&out, "stale: contents\n").unwrap();
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "--force", "--output"])
        .arg(&out)
        .assert()
        .success();
    let body = fs::read_to_string(&out).unwrap();
    assert!(!body.contains("stale: contents"));
    assert!(body.contains("type: rest"));
}

#[test]
fn init_output_is_valid_yaml() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "--source", "rest", "--sink", "jsonl", "--output"])
        .arg(&out)
        .assert()
        .success();
    let body = fs::read_to_string(&out).unwrap();
    // The scaffold itself parses as YAML even before the user fills in the
    // REQUIRED fields — the placeholders are valid YAML values. (Semantic
    // validation via `faucet validate` would still fail because of the empty
    // `base_url`, but `serde_yaml` should consume the structure.)
    serde_yaml::from_str::<serde_yaml::Value>(&body).expect("init output should parse as YAML");
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
        .stdout(contains("sink=jsonl"))
        .stdout(contains("rows=1"));
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
        .stdout(contains("wrote 2 records"))
        .stdout(contains("1 invocation"));

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
pipeline:
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
fn run_auto_discovers_faucet_yaml_and_dotenv_in_cwd() {
    // #55: cwd-based config + .env auto-discovery. `faucet run` with no
    // positional path picks up `faucet.yaml`, and `${env:VAR}` resolves
    // against a `.env` in the same directory.
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name\nzed\n").unwrap();
    fs::write(
        dir.path().join(".env"),
        format!("DISCOVERED_OUT={}\n", out.display()),
    )
    .unwrap();
    fs::write(
        dir.path().join("faucet.yaml"),
        format!(
            r#"version: 1
pipeline:
  source:
    type: csv
    config:
      path: {csv}
  sink:
    type: jsonl
    config:
      path: ${{env:DISCOVERED_OUT}}
"#,
            csv = csv.display(),
        ),
    )
    .unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .current_dir(dir.path())
        .env_remove("DISCOVERED_OUT")
        .arg("run")
        .assert()
        .success()
        .stdout(contains("wrote 1 record"));

    assert!(out.exists(), "auto-discovered run should produce output");
}

#[test]
fn run_with_no_config_and_no_from_env_errors() {
    // No positional path, no --from-env, no faucet.* in cwd → clear error.
    let dir = TempDir::new().unwrap();
    Command::cargo_bin("faucet")
        .unwrap()
        .current_dir(dir.path())
        .arg("run")
        .assert()
        .failure()
        .stderr(contains("no pipeline config"));
}

#[test]
fn run_no_env_file_skips_dotenv_auto_load() {
    // With --no-env-file, a present .env must NOT be loaded. We prove this by
    // requiring an env var that is only defined in .env, and asserting failure.
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    fs::write(&csv, "name\nx\n").unwrap();
    fs::write(
        dir.path().join(".env"),
        "FAUCET_TEST_SKIPPED_PATH=/tmp/should-not-be-read.jsonl\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("faucet.yaml"),
        format!(
            r#"version: 1
pipeline:
  source:
    type: csv
    config:
      path: {csv}
  sink:
    type: jsonl
    config:
      path: ${{env:FAUCET_TEST_SKIPPED_PATH}}
"#,
            csv = csv.display(),
        ),
    )
    .unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .current_dir(dir.path())
        .env_remove("FAUCET_TEST_SKIPPED_PATH")
        .args(["run", "--no-env-file"])
        .assert()
        .failure()
        .stderr(contains("FAUCET_TEST_SKIPPED_PATH"));
}

#[test]
fn init_with_template_flag_names_the_template() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("p.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "--template", "users_api", "--output"])
        .arg(&out)
        .assert()
        .success();
    let body = fs::read_to_string(&out).unwrap();
    assert!(
        body.contains("  sources:\n    users_api:"),
        "expected `  sources:\\n    users_api:` in:\n{body}"
    );
    assert!(
        body.contains("  sinks:\n    users_api:"),
        "expected `  sinks:\\n    users_api:` in:\n{body}"
    );
}

#[test]
fn missing_env_var_in_config_is_reported() {
    let dir = TempDir::new().unwrap();
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(
        &cfg,
        r#"version: 1
pipeline:
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

#[test]
fn init_output_loads_and_expands() {
    // Regression guard: ensure `faucet init` produces a YAML file that
    // PipelineConfig::from_path + expand() accept without error. This catches
    // indent / structural bugs (e.g. CONFIG_INDENT at the wrong depth) that
    // substring assertions miss — a misplaced indent causes the connector config
    // to parse as `null`, but the kinds still appear as sibling keys, so
    // body.contains("base_url") would pass while the config is semantically wrong.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("p.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "--source", "rest", "--sink", "jsonl", "--output"])
        .arg(&path)
        .assert()
        .success();

    let cfg = faucet_cli::config::PipelineConfig::from_path(&path)
        .expect("init output must load via PipelineConfig::from_path");
    let nodes = faucet_cli::expand::expand(&cfg).expect("init output must expand cleanly");
    assert_eq!(nodes.len(), 1, "expected exactly one expanded node");
    assert_eq!(nodes[0].source.kind, "rest");
    assert_eq!(nodes[0].sink.kind, "jsonl");
    // Crucially: the connector config must be properly nested under `config:`,
    // not floated up as siblings. Verify the source config is a non-null object
    // (an empty object would also signal structural breakage).
    assert!(
        nodes[0].source.config.is_object(),
        "source config must be a JSON object (got {:?}); \
         likely a CONFIG_INDENT bug causing fields to float above `config:`",
        nodes[0].source.config
    );
}
