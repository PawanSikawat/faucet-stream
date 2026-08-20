//! End-to-end wiring for the discovery-driven request matrix (#501): a
//! `discover:` row enumerates a value-set from a live endpoint, and a
//! `for_each:` row fans out over the cartesian product of those dimensions —
//! one invocation per tuple, with `${dim.alias}` resolved into the source URL
//! and sink path. Discovery + fan-out rows share one `pipeline.sources`
//! template (the `ref` path), overriding only path / records_path / query.

use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, run_expanded};
use faucet_cli::expand::expand;
use std::path::Path;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn opts() -> ExecuteOptions {
    ExecuteOptions {
        pipeline_name: "disc_test".into(),
        run_id: None,
        execution: None,
        dry_run: false,
        limit: None,
        state_path_override: None,
        shard: None,
        auth: Default::default(),
        clock: chrono::Utc::now().fixed_offset(),
        cancel: None,
        resilience: None,
        sla: None,
        reconcile: None,
        #[cfg(feature = "lineage")]
        lineage: None,
        #[cfg(feature = "lineage")]
        lineage_cfg: None,
        #[cfg(feature = "notify")]
        notifier: None,
        #[cfg(feature = "catalog")]
        catalog: None,
    }
}

fn read_ids(path: &Path) -> Vec<i64> {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["v"]
                .as_i64()
                .unwrap()
        })
        .collect()
}

/// A complete `pipeline.sources.api` rest template block (all rest-required
/// fields), so discovery + fan-out rows can `ref: api` and override only the
/// path / records_path / query_params deltas.
fn api_template(uri: &str) -> String {
    format!(
        r#"  sources:
    api:
      type: rest
      config:
        method: GET
        auth: {{ type: none }}
        base_url: "{uri}"
        path: "/"
        query_params: {{}}
        pagination: {{ type: None }}
        max_retries: 0
        retry_backoff: 1
        tolerated_http_errors: []
        replication_method: {{ type: FullTable }}
        primary_keys: []
        partitions: []
        schema_sample_size: 100
"#
    )
}

#[tokio::test]
async fn single_dimension_fan_out() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/subsidiaries"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "subsidiaries": [{"id": 10}, {"id": 20}]
        })))
        .mount(&server)
        .await;
    for sid in [10, 20] {
        Mock::given(method("GET"))
            .and(path("/report"))
            .and(query_param("subsidiary_id", sid.to_string()))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "rows": [{"v": sid}]
            })))
            .mount(&server)
            .await;
    }

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let yaml = format!(
        r#"version: 1
name: disc_test
pipeline:
{template}  sink:
    type: jsonl
    config: {{ path: "{out}/${{subs.subsidiary_id}}.jsonl" }}
matrix:
  - id: subs
    discover:
      source:
        ref: api
        config: {{ path: "/subsidiaries", records_path: "$.subsidiaries[*]" }}
      select: "$.id"
      as: subsidiary_id
  - id: report
    for_each: [subs]
    source:
      ref: api
      config:
        path: "/report"
        records_path: "$.rows[*]"
        query_params: {{ subsidiary_id: "${{subs.subsidiary_id}}" }}
"#,
        template = api_template(&server.uri()),
        out = out.display()
    );

    let cfg = PipelineConfig::from_text(&yaml, Path::new("disc.yaml")).unwrap();
    let nodes = expand(&cfg).unwrap();
    let summary = run_expanded(nodes, opts()).await.unwrap();
    assert!(!summary.had_failures(), "run failed: {summary:?}");

    assert_eq!(read_ids(&out.join("10.jsonl")), vec![10]);
    assert_eq!(read_ids(&out.join("20.jsonl")), vec![20]);
}

#[tokio::test]
async fn empty_discovery_skips_the_fan_out_row() {
    let server = MockServer::start().await;
    // Discovery returns no subsidiaries → the product has zero tuples.
    Mock::given(method("GET"))
        .and(path("/subsidiaries"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "subsidiaries": []
        })))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let yaml = format!(
        r#"version: 1
name: disc_test
pipeline:
{template}  sink:
    type: jsonl
    config: {{ path: "{out}/${{subs.subsidiary_id}}.jsonl" }}
matrix:
  - id: subs
    discover:
      source: {{ ref: api, config: {{ path: "/subsidiaries", records_path: "$.subsidiaries[*]" }} }}
      select: "$.id"
      as: subsidiary_id
  - id: report
    for_each: [subs]
    source:
      ref: api
      config: {{ path: "/report", records_path: "$.rows[*]" }}
"#,
        template = api_template(&server.uri()),
        out = out.display()
    );

    let cfg = PipelineConfig::from_text(&yaml, Path::new("disc.yaml")).unwrap();
    let nodes = expand(&cfg).unwrap();
    let summary = run_expanded(nodes, opts()).await.unwrap();
    // Discovery succeeded (0 values); the fan-out row produced no invocations
    // and the run did not fail.
    assert!(!summary.had_failures(), "run failed: {summary:?}");
    assert!(
        std::fs::read_dir(&out).unwrap().next().is_none(),
        "no output expected"
    );
}

#[tokio::test]
async fn cross_product_of_two_dimensions() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/subsidiaries"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "subsidiaries": [{"id": 1}, {"id": 2}]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/fields"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "fields": [{"code": 100}, {"code": 200}]
        })))
        .mount(&server)
        .await;
    for sid in [1, 2] {
        for fld in [100, 200] {
            Mock::given(method("GET"))
                .and(path("/report"))
                .and(query_param("subsidiary_id", sid.to_string()))
                .and(query_param("field", fld.to_string()))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "rows": [{"v": sid * 1000 + fld}]
                })))
                .mount(&server)
                .await;
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let yaml = format!(
        r#"version: 1
name: disc_test
pipeline:
{template}  sink:
    type: jsonl
    config: {{ path: "{out}/${{subs.subsidiary_id}}-${{flds.field}}.jsonl" }}
matrix:
  - id: subs
    discover:
      source: {{ ref: api, config: {{ path: "/subsidiaries", records_path: "$.subsidiaries[*]" }} }}
      select: "$.id"
      as: subsidiary_id
  - id: flds
    discover:
      source: {{ ref: api, config: {{ path: "/fields", records_path: "$.fields[*]" }} }}
      select: "$.code"
      as: field
  - id: report
    for_each: [subs, flds]
    source:
      ref: api
      config:
        path: "/report"
        records_path: "$.rows[*]"
        query_params: {{ subsidiary_id: "${{subs.subsidiary_id}}", field: "${{flds.field}}" }}
"#,
        template = api_template(&server.uri()),
        out = out.display()
    );

    let cfg = PipelineConfig::from_text(&yaml, Path::new("disc.yaml")).unwrap();
    let nodes = expand(&cfg).unwrap();
    let summary = run_expanded(nodes, opts()).await.unwrap();
    assert!(!summary.had_failures(), "run failed: {summary:?}");

    assert_eq!(read_ids(&out.join("1-100.jsonl")), vec![1100]);
    assert_eq!(read_ids(&out.join("1-200.jsonl")), vec![1200]);
    assert_eq!(read_ids(&out.join("2-100.jsonl")), vec![2100]);
    assert_eq!(read_ids(&out.join("2-200.jsonl")), vec![2200]);
}
