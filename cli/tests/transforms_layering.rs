//! End-to-end coverage of the three-layer transforms model and the
//! `inherit_transforms` opt-out, exercised through the full CLI run path.

use faucet_cli::config::PipelineConfig;
use faucet_cli::expand::expand;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn end_to_end_three_layer_pipeline() {
    let dir = tempdir().unwrap();
    let out_path = dir.path().join("out.jsonl");
    let yaml = format!(
        r#"
version: 1
name: layering_e2e
pipeline:
  transforms:
    - {{ type: set, config: {{ values: {{ _layer: pipeline }} }} }}
  sources:
    s:
      type: rest
      config:
        base_url: https://example.com
        path: /x
        method: GET
        auth: {{ type: None }}
        query_params: {{}}
        pagination: {{ type: None }}
        max_retries: 0
        retry_backoff: 0
        tolerated_http_errors: []
        replication_method: {{ type: FullTable }}
        primary_keys: []
        partitions: []
        schema_sample_size: 0
      transforms:
        - {{ type: keys_case, config: {{ mode: snake }} }}
  sink:
    type: jsonl
    config:
      destination: "{}"
matrix:
  - id: row_default
    source: {{ ref: s }}
    transforms:
      - {{ type: select, config: {{ fields: [id] }} }}
  - id: row_no_inherit
    source: {{ ref: s }}
    inherit_transforms: false
    transforms:
      - {{ type: flatten, config: {{ separator: "_" }} }}
"#,
        out_path.display()
    );

    let cfg = PipelineConfig::from_text(&yaml, Path::new("test.yaml")).unwrap();
    let nodes = expand(&cfg).unwrap();
    assert_eq!(nodes.len(), 2);

    let row_default = nodes.iter().find(|n| n.id == "row_default").unwrap();
    let kinds: Vec<&str> = row_default
        .transforms
        .iter()
        .map(|t| t.kind.as_str())
        .collect();
    assert_eq!(kinds, vec!["set", "keys_case", "select"]);

    let row_no_inherit = nodes.iter().find(|n| n.id == "row_no_inherit").unwrap();
    let kinds: Vec<&str> = row_no_inherit
        .transforms
        .iter()
        .map(|t| t.kind.as_str())
        .collect();
    assert_eq!(kinds, vec!["flatten"]);
}

#[test]
fn sink_with_transforms_field_is_rejected() {
    let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sinks:
    bad:
      type: jsonl
      config: { destination: /tmp/x.jsonl }
      transforms:
        - { type: set, config: { values: { x: 1 } } }
matrix:
  - id: r
    sink: { ref: bad }
"#;
    let cfg = PipelineConfig::from_text(yaml, Path::new("test.yaml")).unwrap();
    let err = expand(&cfg).expect_err("sinks must reject transforms");
    let s = format!("{err}");
    assert!(s.contains("sink template 'bad'"), "got: {s}");
}

#[test]
fn anonymous_single_row_inherits_defaults() {
    // No matrix: should still resolve to a single ExpandedNode with the
    // pipeline-level transforms applied.
    let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink: { type: jsonl, config: { destination: /tmp/x.jsonl } }
  transforms:
    - { type: set, config: { values: { x: 1 } } }
"#;
    let cfg = PipelineConfig::from_text(yaml, Path::new("test.yaml")).unwrap();
    let nodes = expand(&cfg).unwrap();
    assert_eq!(nodes.len(), 1);
    let kinds: Vec<&str> = nodes[0]
        .transforms
        .iter()
        .map(|t| t.kind.as_str())
        .collect();
    assert_eq!(kinds, vec!["set"]);
}

#[test]
fn cli_example_yaml_validates() {
    // Sanity: the bundled example YAML expands without errors.
    let yaml = include_str!("../examples/rest_to_stdout_transforms.yaml");
    let cfg = PipelineConfig::from_text(yaml, Path::new("rest_to_stdout_transforms.yaml"))
        .expect("example parses");
    let nodes = expand(&cfg).expect("example expands");
    // Each expanded row should have a non-empty transforms list (the example uses all three layers).
    for n in &nodes {
        for t in &n.transforms {
            assert!(!t.kind.is_empty(), "row {}: empty transform kind", n.id);
        }
    }
}

#[test]
fn cli_example_filter_explode_yaml_validates() {
    // Sanity: the filter+explode demo YAML parses, expands, and produces a
    // transform list that includes the new filter / explode kinds plus
    // keys_case from the row-level transforms.
    let yaml = include_str!("../examples/rest_filter_explode_to_stdout.yaml");
    let cfg = PipelineConfig::from_text(yaml, Path::new("rest_filter_explode_to_stdout.yaml"))
        .expect("example parses");
    let nodes = expand(&cfg).expect("example expands");
    assert!(!nodes.is_empty(), "example must expand to at least one row");
    let kinds: Vec<&str> = nodes[0]
        .transforms
        .iter()
        .map(|t| t.kind.as_str())
        .collect();
    assert!(
        kinds.contains(&"filter"),
        "expected filter in row stages, got {kinds:?}"
    );
    assert!(
        kinds.contains(&"explode"),
        "expected explode in row stages, got {kinds:?}"
    );
    assert!(
        kinds.contains(&"keys_case"),
        "expected keys_case in row stages, got {kinds:?}"
    );
}
