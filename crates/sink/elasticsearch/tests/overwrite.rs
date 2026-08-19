//! Integration tests for the Elasticsearch sink's `write_mode: overwrite` (#494)
//! — the alias-swap lifecycle, driven against a wiremock cluster so the exact
//! HTTP calls (create staging → bulk into staging → refresh → atomic `_aliases`
//! swap → drop old) can be asserted without Docker.

use faucet_core::{Sink, WriteMode, WriteSpec};
use faucet_sink_elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use serde_json::{Value, json};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

const INDEX: &str = "orders";

fn overwrite_sink(base_url: &str) -> ElasticsearchSink {
    let mut cfg = ElasticsearchSinkConfig::new(base_url, INDEX);
    cfg.write = WriteSpec {
        write_mode: WriteMode::Overwrite,
        ..Default::default()
    };
    ElasticsearchSink::new(cfg).unwrap()
}

async fn ok(server: &MockServer, m: &str, p_regex: &str, body: Value) {
    Mock::given(method(m))
        .and(path_regex(p_regex))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

async fn queries_to(server: &MockServer, suffix: &str) -> Vec<wiremock::http::Method> {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().ends_with(suffix))
        .map(|r| r.method)
        .collect()
}

#[tokio::test]
async fn overwrite_stages_then_swaps_alias() {
    let server = MockServer::start().await;
    // Existing alias `orders` → one physical index `orders-old`.
    Mock::given(method("GET"))
        .and(path(format!("/_alias/{INDEX}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "orders-old": { "aliases": { "orders": {} } }
        })))
        .mount(&server)
        .await;
    // Mappings of the current target (copied onto staging).
    Mock::given(method("GET"))
        .and(path("/orders-old/_mapping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "orders-old": { "mappings": { "properties": { "id": { "type": "long" } } } }
        })))
        .mount(&server)
        .await;
    // Create staging (PUT /orders-faucet-ovw-*), refresh, delete old, _bulk, swap.
    ok(
        &server,
        "PUT",
        r"^/orders-faucet-ovw-.*$",
        json!({"acknowledged": true}),
    )
    .await;
    ok(
        &server,
        "POST",
        r"^/orders-faucet-ovw-.*/_refresh$",
        json!({}),
    )
    .await;
    ok(
        &server,
        "DELETE",
        r"^/orders-old$",
        json!({"acknowledged": true}),
    )
    .await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"errors": false, "items": []})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .mount(&server)
        .await;

    let sink = overwrite_sink(&server.uri());
    sink.begin_overwrite().await.expect("begin");
    let n = sink
        .write_batch(&[json!({"id": 1}), json!({"id": 2})])
        .await
        .expect("write");
    assert_eq!(n, 2);
    sink.commit_overwrite().await.expect("commit");

    // The staging index was created and the alias was swapped + old dropped.
    assert!(
        !queries_to(&server, "/_aliases").await.is_empty(),
        "alias swap posted"
    );
    assert_eq!(
        queries_to(&server, "/orders-old").await.len(),
        1,
        "old index deleted"
    );

    // The bulk write targeted the staging index, not the alias.
    let reqs = server.received_requests().await.unwrap();
    let bulk = reqs
        .iter()
        .find(|r| r.url.path() == "/_bulk")
        .expect("a _bulk call");
    let body = String::from_utf8_lossy(&bulk.body);
    assert!(
        body.contains("orders-faucet-ovw-"),
        "bulk must target the staging index, got: {body}"
    );
    assert!(
        !body.contains("\"_index\":\"orders\""),
        "bulk must not target the alias directly"
    );

    // The alias-swap body removes the old target and adds the staging one.
    let swap = reqs.iter().find(|r| r.url.path() == "/_aliases").unwrap();
    let swap_body: Value = serde_json::from_slice(&swap.body).unwrap();
    let actions = swap_body["actions"].as_array().unwrap();
    assert!(actions.iter().any(|a| a["remove"]["index"] == "orders-old"));
    assert!(actions.iter().any(|a| {
        a["add"]["index"]
            .as_str()
            .is_some_and(|s| s.starts_with("orders-faucet-ovw-"))
    }));
}

#[tokio::test]
async fn overwrite_refuses_a_concrete_index() {
    let server = MockServer::start().await;
    // No alias named `orders` …
    Mock::given(method("GET"))
        .and(path(format!("/_alias/{INDEX}")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    // … but a concrete index of that name exists.
    Mock::given(method("HEAD"))
        .and(path(format!("/{INDEX}")))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let sink = overwrite_sink(&server.uri());
    let err = sink
        .begin_overwrite()
        .await
        .expect_err("concrete index must be refused");
    assert!(err.to_string().contains("concrete index"), "{err}");
}

#[tokio::test]
async fn overwrite_first_run_creates_alias() {
    let server = MockServer::start().await;
    // Neither an alias nor a concrete index exists yet.
    Mock::given(method("GET"))
        .and(path(format!("/_alias/{INDEX}")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("HEAD"))
        .and(path(format!("/{INDEX}")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    ok(
        &server,
        "PUT",
        r"^/orders-faucet-ovw-.*$",
        json!({"acknowledged": true}),
    )
    .await;
    ok(
        &server,
        "POST",
        r"^/orders-faucet-ovw-.*/_refresh$",
        json!({}),
    )
    .await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"errors": false, "items": []})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .mount(&server)
        .await;

    let sink = overwrite_sink(&server.uri());
    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 1})]).await.expect("write");
    sink.commit_overwrite().await.expect("commit");

    // First run: the swap only adds (no remove).
    let reqs = server.received_requests().await.unwrap();
    let swap = reqs.iter().find(|r| r.url.path() == "/_aliases").unwrap();
    let swap_body: Value = serde_json::from_slice(&swap.body).unwrap();
    let actions = swap_body["actions"].as_array().unwrap();
    assert_eq!(actions.len(), 1);
    assert!(actions[0].get("add").is_some());
}

#[tokio::test]
async fn overwrite_abort_drops_staging_without_swap() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/_alias/{INDEX}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "orders-old": { "aliases": { "orders": {} } }
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/orders-old/_mapping"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"orders-old": {"mappings": {}}})),
        )
        .mount(&server)
        .await;
    ok(
        &server,
        "PUT",
        r"^/orders-faucet-ovw-.*$",
        json!({"acknowledged": true}),
    )
    .await;
    ok(
        &server,
        "DELETE",
        r"^/orders-faucet-ovw-.*$",
        json!({"acknowledged": true}),
    )
    .await;

    let sink = overwrite_sink(&server.uri());
    sink.begin_overwrite().await.expect("begin");
    sink.abort_overwrite().await.expect("abort");

    // Abort deletes the staging index and never touches the alias.
    let reqs = server.received_requests().await.unwrap();
    assert!(
        reqs.iter()
            .any(|r| r.method == wiremock::http::Method::DELETE
                && r.url.path().contains("orders-faucet-ovw-")),
        "abort must delete the staging index"
    );
    assert!(
        !reqs.iter().any(|r| r.url.path() == "/_aliases"),
        "abort must not swap the alias"
    );
}
