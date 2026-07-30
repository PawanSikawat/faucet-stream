//! Topology mode (#71 / #72) end-to-end tests: fan-out (tee), fan-in (merge),
//! and cross-source join, plus the config-validation error paths.
//!
//! Binary-driven tests exercise the `run` / `validate` / `preview` command
//! wiring via `assert_cmd`; the error-path tests call
//! `faucet_cli::topology::build_topology` directly to assert the typed
//! `CliError` variants.
#![cfg(all(
    feature = "source-csv",
    feature = "sink-jsonl",
    feature = "sink-stdout",
    feature = "transforms"
))]

use assert_cmd::Command;
use predicates::str::contains;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

fn write(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
}

fn orders_csv(dir: &Path) -> std::path::PathBuf {
    let p = dir.join("orders.csv");
    write(
        &p,
        "order_id,country_code,amount\n1,US,10\n2,US,5\n3,IN,7\n4,DE,3\n",
    );
    p
}

fn countries_csv(dir: &Path) -> std::path::PathBuf {
    let p = dir.join("countries.csv");
    write(&p, "code,country\nUS,United States\nIN,India\nDE,Germany\n");
    p
}

// ── tee (fan-out) ─────────────────────────────────────────────────────────────

#[test]
fn tee_fans_out_to_two_sinks() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let a = dir.path().join("a.jsonl");
    let b = dir.path().join("b.jsonl");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: tee_test
pipeline:
  sources:
    orders: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    a: {{ type: jsonl, config: {{ path: {a} }} }}
    b: {{ type: jsonl, config: {{ path: {b} }} }}
  nodes:
    # `config` inline override (deep-merged onto the template) + a transform node.
    src: {{ kind: source, ref: orders, config: {{ batch_size: 2 }} }}
    norm: {{ kind: transform, transforms: [ {{ type: keys_case, config: {{ mode: snake }} }} ] }}
    fan: {{ kind: tee, channel_capacity: 2, fanout: 2 }}
    # `type` inline override (same kind as the template — exercises the override path).
    wa: {{ kind: sink, ref: a, type: jsonl }}
    wb: {{ kind: sink, ref: b }}
  edges:
    - {{ from: src, to: norm }}
    - {{ from: norm, to: fan }}
    - {{ from: fan, to: wa }}
    - {{ from: fan, to: wb }}
"#,
            csv = csv.display(),
            a = a.display(),
            b = b.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success()
        .stderr(contains("2 sink node"));

    assert_eq!(fs::read_to_string(&a).unwrap().lines().count(), 4);
    assert_eq!(fs::read_to_string(&b).unwrap().lines().count(), 4);
}

// ── merge (fan-in) ────────────────────────────────────────────────────────────

#[test]
fn merge_fans_in_two_sources() {
    let dir = TempDir::new().unwrap();
    let orders = orders_csv(dir.path());
    let countries = countries_csv(dir.path());
    let out = dir.path().join("combined.jsonl");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: merge_test
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {orders} }} }}
    c: {{ type: csv, config: {{ path: {countries} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    ro: {{ kind: source, ref: o }}
    rc: {{ kind: source, ref: c }}
    m: {{ kind: merge }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: ro, to: m }}
    - {{ from: rc, to: m }}
    - {{ from: m, to: w }}
"#,
            orders = orders.display(),
            countries = countries.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success();

    // 4 orders + 3 countries.
    assert_eq!(fs::read_to_string(&out).unwrap().lines().count(), 7);
}

// ── join ───────────────────────────────────────────────────────────────────────

#[test]
fn join_enriches_orders_with_country() {
    let dir = TempDir::new().unwrap();
    let orders = orders_csv(dir.path());
    let countries = countries_csv(dir.path());
    let out = dir.path().join("enriched.jsonl");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: join_test
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {orders} }} }}
    c: {{ type: csv, config: {{ path: {countries} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    ro: {{ kind: source, ref: o }}
    rc: {{ kind: source, ref: c }}
    j:
      kind: join
      mode: left
      build: {{ edge: c_in, key: code }}
      probe: {{ edge: o_in, key: country_code }}
      project:
        - {{ from: country, as: country_name }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: rc, to: j, as: c_in }}
    - {{ from: ro, to: j, as: o_in }}
    - {{ from: j, to: w }}
"#,
            orders = orders.display(),
            countries = countries.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success();

    let body = fs::read_to_string(&out).unwrap();
    assert_eq!(body.lines().count(), 4);
    assert!(body.contains("\"country_name\":\"United States\""));
    assert!(body.contains("\"country_name\":\"India\""));
    assert!(body.contains("\"country_name\":\"Germany\""));
}

// ── validate + preview ──────────────────────────────────────────────────────────

#[test]
fn validate_reports_topology_summary() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let out = dir.path().join("o.jsonl");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: validate_test
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            csv = csv.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("2 node(s), 1 edge(s) — valid"));
}

#[test]
fn preview_prints_source_records() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let out = dir.path().join("o.jsonl");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: preview_test
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            csv = csv.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["preview", "--limit", "2"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("order_id"));
}

// ── error paths (direct calls) ───────────────────────────────────────────────────

use faucet_cli::auth_catalog::build_auth_catalog;
use faucet_cli::config::PipelineConfig;
use faucet_cli::error::CliError;
use faucet_cli::topology::build_topology;

fn parse(yaml: &str) -> PipelineConfig {
    PipelineConfig::from_text(yaml, Path::new("test.yaml")).expect("parses")
}

#[tokio::test]
async fn rejects_matrix_and_nodes_together() {
    let cfg = parse(
        r#"version: 1
name: both
pipeline:
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    s: { kind: source, ref: o }
    w: { kind: sink, ref: out }
  edges:
    - { from: s, to: w }
matrix:
  - id: extra
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err();
    assert!(matches!(err, CliError::MatrixAndNodesBothPresent));
}

#[tokio::test]
async fn rejects_edge_to_unknown_node() {
    let cfg = parse(
        r#"version: 1
name: badedge
pipeline:
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    s: { kind: source, ref: o }
    w: { kind: sink, ref: out }
  edges:
    - { from: s, to: ghost }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err();
    assert!(
        matches!(err, CliError::EdgeEndpointMissing { ref name, .. } if name == "ghost"),
        "{err:?}"
    );
}

#[tokio::test]
async fn rejects_unknown_template_ref() {
    let cfg = parse(
        r#"version: 1
name: badref
pipeline:
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    s: { kind: source, ref: nonexistent }
    w: { kind: sink, ref: out }
  edges:
    - { from: s, to: w }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err();
    assert!(matches!(err, CliError::UnknownTemplate { .. }), "{err:?}");
}

#[tokio::test]
async fn rejects_edge_from_unknown_node() {
    let cfg = parse(
        r#"version: 1
name: badfrom
pipeline:
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    s: { kind: source, ref: o }
    w: { kind: sink, ref: out }
  edges:
    - { from: ghost, to: w }
    - { from: s, to: w }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err();
    assert!(
        matches!(err, CliError::EdgeEndpointMissing { ref name, .. } if name == "ghost"),
        "{err:?}"
    );
}

#[tokio::test]
async fn preview_rejects_matrix_and_nodes() {
    let cfg = parse(
        r#"version: 1
name: pv_both
pipeline:
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    s: { kind: source, ref: o }
    w: { kind: sink, ref: out }
  edges:
    - { from: s, to: w }
matrix:
  - id: extra
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = faucet_cli::topology::preview(&cfg, &auth, 5)
        .await
        .unwrap_err();
    assert!(matches!(err, CliError::MatrixAndNodesBothPresent));
}

#[tokio::test]
async fn preview_errors_when_no_source_node() {
    // A nodes map with no `source` kind: preview finds nothing to emit.
    let cfg = parse(
        r#"version: 1
name: pv_nosrc
pipeline:
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    w: { kind: sink, ref: out }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = faucet_cli::topology::preview(&cfg, &auth, 5)
        .await
        .unwrap_err();
    assert!(matches!(err, CliError::InvalidTopology { .. }), "{err:?}");
}

#[tokio::test]
async fn run_topology_direct_with_uncancelled_token() {
    // Exercises `run_topology` directly (records-written mapping + the cancel
    // branch) with a non-cancelled token.
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let out = dir.path().join("o.jsonl");
    let cfg = parse(&format!(
        r#"version: 1
name: direct_run
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
        csv = csv.display(),
        out = out.display()
    ));
    let auth = build_auth_catalog(None).unwrap();
    let cancel = faucet_core::CancellationToken::new();
    let summary = faucet_cli::topology::run_topology(&cfg, &auth, Some(cancel))
        .await
        .unwrap();
    assert_eq!(summary.invocations.len(), 1);
    assert_eq!(summary.invocations[0].records_written, 4);
    assert_eq!(summary.invocations[0].row_id, "w");
}

#[tokio::test]
async fn rejects_missing_default_template() {
    // A source node with no `ref` defaults to the `default` template; with only
    // a named template and no legacy `pipeline.source`, that is a MissingTemplate.
    let cfg = parse(
        r#"version: 1
name: nodefault
pipeline:
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    s: { kind: source }
    w: { kind: sink, ref: out }
  edges:
    - { from: s, to: w }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err();
    assert!(matches!(err, CliError::MissingTemplate { .. }), "{err:?}");
}

// ── run-command coverage: state, on_error, output formats, failures ──────────────

#[test]
fn run_with_state_and_stop_on_error() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let out = dir.path().join("o.jsonl");
    let state = dir.path().join("state");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: state_stop
execution:
  on_error: stop
pipeline:
  state: {{ type: file, config: {{ path: {state} }} }}
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            state = state.display(),
            csv = csv.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success();
    assert_eq!(fs::read_to_string(&out).unwrap().lines().count(), 4);
}

#[test]
fn run_output_json_and_ndjson() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let out = dir.path().join("o.jsonl");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: outfmt
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            csv = csv.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", "--output", "json"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("totals"));

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", "--output", "ndjson"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("\"row_id\""));
}

#[test]
fn run_with_dlq_block() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let out = dir.path().join("o.jsonl");
    let dlq = dir.path().join("dlq.jsonl");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: dlq_topo
pipeline:
  dlq:
    sink: {{ type: jsonl, config: {{ path: {dlq} }} }}
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            dlq = dlq.display(),
            csv = csv.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success();
    // Happy path: everything written to the main sink, nothing to the DLQ.
    assert_eq!(fs::read_to_string(&out).unwrap().lines().count(), 4);
}

#[test]
fn run_reports_node_failure_under_continue() {
    // A source pointed at a missing file fails; under the default `continue`
    // policy the run exits non-zero with a TopologyHadFailures error.
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("o.jsonl");
    let missing = dir.path().join("does_not_exist.csv");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: failrun
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {missing} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            missing = missing.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .failure();
}

#[tokio::test]
async fn rejects_invalid_graph_arity() {
    // A source with an incoming edge is an arity violation caught by the core
    // validator and surfaced as InvalidTopology.
    let cfg = parse(
        r#"version: 1
name: badarity
pipeline:
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    s: { kind: source, ref: o }
    w: { kind: sink, ref: out }
  edges:
    - { from: s, to: w }
    - { from: w, to: s }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err();
    assert!(matches!(err, CliError::InvalidTopology { .. }), "{err:?}");
}
