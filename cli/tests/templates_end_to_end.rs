//! End-to-end test for the named-templates feature: vars + sources/sinks
//! catalogs + matrix `ref:` + parent-child fan-out + load-time
//! ${sources.X.PATH} reference.

use faucet_cli::config::PipelineConfig;
use faucet_cli::expand::{expand, NodeRole};

#[test]
fn templated_config_resolves_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pipeline.yaml");
    std::fs::write(
        &path,
        r#"
version: 1
name: e2e
vars:
  api_base: https://api.example.com

pipeline:
  sources:
    users_api:
      type: rest
      config:
        base_url: ${vars.api_base}
        records_path: $.data[*]
    posts_api:
      type: rest
      config:
        base_url: ${vars.api_base}
        records_path: $.data[*]
  sinks:
    archive: { type: jsonl, config: { append: false } }

matrix:
  - id: load_users
    source:
      ref: users_api
      config:
        path: /v1/users
        # Use a load-time template ref to copy the base_url onto an audit field.
        audit_url: ${sources.users_api.config.base_url}/audit
    sink:
      ref: archive
      config: { path: ./users.jsonl }

  - id: load_posts
    parent: load_users
    parent_key: id
    source:
      ref: posts_api
      config: { path: "/v1/users/${load_users.id}/posts" }
    sink:
      ref: archive
      config: { path: "posts-${load_users.id}.jsonl" }
"#,
    )
    .unwrap();

    let cfg = PipelineConfig::from_path(&path).unwrap();
    // Vars resolved.
    assert_eq!(
        cfg.pipeline.sources["users_api"].config["base_url"],
        "https://api.example.com"
    );
    // Load-time template ref resolved.
    let row0 = cfg.matrix[0].source.as_ref().unwrap();
    assert_eq!(
        row0.config.as_ref().unwrap()["audit_url"],
        "https://api.example.com/audit"
    );

    let nodes = expand(&cfg).unwrap();
    assert_eq!(nodes.len(), 2);
    let users = nodes.iter().find(|n| n.id == "load_users").unwrap();
    assert_eq!(users.source.kind, "rest");
    assert_eq!(users.source.config["base_url"], "https://api.example.com");
    assert_eq!(users.source.config["path"], "/v1/users");

    let posts = nodes.iter().find(|n| n.id == "load_posts").unwrap();
    assert!(matches!(posts.role, NodeRole::Child { .. }));
    assert_eq!(posts.source.kind, "rest");
    // ${load_users.id} survived to runtime.
    assert_eq!(
        posts.source.config["path"],
        "/v1/users/${load_users.id}/posts"
    );
    // Deferred refs: source path + sink path both reference load_users.id.
    assert_eq!(posts.deferred_refs.len(), 2);
}
