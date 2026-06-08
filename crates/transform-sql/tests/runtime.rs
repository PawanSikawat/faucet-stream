//! Integration tests for the compiled `SqlTransform`: per-page execution,
//! reference relations, schema drift, validation, and connection reuse.

use faucet_core::stage::{CompiledStage, apply_stages_to_page, compile_stage};
use faucet_transform_sql::{RelationSource, RelationSpec, SqlTransform, SqlTransformConfig};
use serde_json::{Value, json};

/// Compile a config into a runnable page stage.
fn stage(cfg: SqlTransformConfig) -> CompiledStage {
    compile_stage(&SqlTransform::compile(&cfg).unwrap().into_page_stage()).unwrap()
}

/// Convenience: compile a bare query (no relations) and run one page through it.
fn run(query: &str, page: Vec<Value>) -> Vec<Value> {
    let s = stage(SqlTransformConfig {
        query: query.into(),
        relations: vec![],
        memory_limit: None,
        threads: None,
    });
    apply_stages_to_page(page, &[s]).unwrap()
}

#[test]
fn select_passthrough_and_projection() {
    let out = run(
        "SELECT id, upper(name) AS name FROM batch WHERE id > 1",
        vec![json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})],
    );
    assert_eq!(out, vec![json!({"id": 2, "name": "B"})]);
}

#[test]
fn group_by_aggregation_within_page() {
    let out = run(
        "SELECT k, SUM(v) AS total FROM batch GROUP BY k ORDER BY k",
        vec![
            json!({"k": "a", "v": 1}),
            json!({"k": "a", "v": 2}),
            json!({"k": "b", "v": 5}),
        ],
    );
    assert_eq!(
        out,
        vec![json!({"k": "a", "total": 3}), json!({"k": "b", "total": 5})]
    );
}

#[test]
fn empty_page_returns_empty_without_error() {
    let out = run("SELECT * FROM batch", vec![]);
    assert!(out.is_empty());
}

#[test]
fn empty_result_set_is_valid() {
    let out = run("SELECT * FROM batch WHERE false", vec![json!({"a": 1})]);
    assert!(out.is_empty());
}

#[test]
fn schema_drift_recreates_batch() {
    let s = stage(SqlTransformConfig {
        query: "SELECT * FROM batch".into(),
        relations: vec![],
        memory_limit: None,
        threads: None,
    });
    let p1 = apply_stages_to_page(vec![json!({"a": 1})], std::slice::from_ref(&s)).unwrap();
    let p2 =
        apply_stages_to_page(vec![json!({"a": 2, "b": "x"})], std::slice::from_ref(&s)).unwrap();
    assert_eq!(p1[0]["a"], json!(1));
    assert_eq!(p2[0]["b"], json!("x"));
}

#[test]
fn join_to_csv_reference_relation() {
    let path = format!("{}/tests/data/countries.csv", env!("CARGO_MANIFEST_DIR"));
    let s = stage(SqlTransformConfig {
        query: "SELECT b.id, c.country FROM batch b LEFT JOIN countries c ON b.code = c.code ORDER BY b.id".into(),
        relations: vec![RelationSpec {
            name: "countries".into(),
            source: RelationSource::Csv { path, has_header: true },
            reload_on_change: false,
        }],
        memory_limit: None,
        threads: None,
    });
    let out = apply_stages_to_page(
        vec![
            json!({"id": 1, "code": "US"}),
            json!({"id": 2, "code": "IN"}),
        ],
        &[s],
    )
    .unwrap();
    assert_eq!(out[0]["country"], json!("United States"));
    assert_eq!(out[1]["country"], json!("India"));
}

#[test]
fn values_relation_join() {
    let s = stage(SqlTransformConfig {
        query: "SELECT b.id, t.label FROM batch b JOIN tiers t ON b.tier = t.id ORDER BY b.id"
            .into(),
        relations: vec![RelationSpec {
            name: "tiers".into(),
            source: RelationSource::Values {
                columns: vec!["id".into(), "label".into()],
                rows: vec![
                    vec![json!(1), json!("gold")],
                    vec![json!(2), json!("silver")],
                ],
            },
            reload_on_change: false,
        }],
        memory_limit: None,
        threads: None,
    });
    let out = apply_stages_to_page(vec![json!({"id": 9, "tier": 2})], &[s]).unwrap();
    assert_eq!(out[0]["label"], json!("silver"));
}

#[test]
fn bad_query_fails_at_compile_with_message() {
    let err = SqlTransform::compile(&SqlTransformConfig {
        query: "SELEKT * FROM batch".into(), // syntax error
        relations: vec![],
        memory_limit: None,
        threads: None,
    })
    .unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(msg.contains("sel") || msg.contains("syntax"), "got: {err}");
}

#[test]
fn query_referencing_only_batch_compiles() {
    // `batch` doesn't exist yet at compile — referencing only its columns must
    // be tolerated (the binder error is about `batch` missing).
    SqlTransform::compile(&SqlTransformConfig {
        query: "SELECT x, y FROM batch WHERE z > 1".into(),
        relations: vec![],
        memory_limit: None,
        threads: None,
    })
    .expect("references only batch columns -> tolerated at compile");
}

#[test]
fn reserved_relation_name_batch_rejected() {
    let err = SqlTransform::compile(&SqlTransformConfig {
        query: "SELECT * FROM batch".into(),
        relations: vec![RelationSpec {
            name: "batch".into(),
            source: RelationSource::Values {
                columns: vec!["a".into()],
                rows: vec![],
            },
            reload_on_change: false,
        }],
        memory_limit: None,
        threads: None,
    })
    .unwrap_err();
    assert!(format!("{err}").contains("batch"));
}

#[test]
fn missing_reference_file_fails_at_compile() {
    let err = SqlTransform::compile(&SqlTransformConfig {
        query: "SELECT * FROM batch JOIN nope USING (id)".into(),
        relations: vec![RelationSpec {
            name: "nope".into(),
            source: RelationSource::Csv {
                path: "/does/not/exist.csv".into(),
                has_header: true,
            },
            reload_on_change: false,
        }],
        memory_limit: None,
        threads: None,
    })
    .unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("exist") || msg.contains("no such") || msg.contains("nope"),
        "got: {err}"
    );
}

#[test]
fn connection_reused_across_pages() {
    let s = stage(SqlTransformConfig {
        query: "SELECT count(*) AS n FROM batch".into(),
        relations: vec![],
        memory_limit: None,
        threads: None,
    });
    for i in 1..=3 {
        let out = apply_stages_to_page(
            (0..i).map(|j| json!({"x": j})).collect(),
            std::slice::from_ref(&s),
        )
        .unwrap();
        assert_eq!(out[0]["n"], json!(i));
    }
}

#[test]
fn aggregation_is_per_page_over_two_pages() {
    let s = stage(SqlTransformConfig {
        query: "SELECT SUM(v) AS total FROM batch".into(),
        relations: vec![],
        memory_limit: None,
        threads: None,
    });
    let p1 = apply_stages_to_page(
        vec![json!({"v": 1}), json!({"v": 2})],
        std::slice::from_ref(&s),
    )
    .unwrap();
    let p2 = apply_stages_to_page(vec![json!({"v": 10})], std::slice::from_ref(&s)).unwrap();
    assert_eq!(p1[0]["total"], json!(3)); // per-page, not global
    assert_eq!(p2[0]["total"], json!(10));
}
