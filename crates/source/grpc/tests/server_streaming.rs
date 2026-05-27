//! Integration tests for `GrpcStream` against a real tonic `EchoService`.
//!
//! Each test starts an in-process server bound to an ephemeral port and
//! drives the source through both the `fetch_all` and `stream_pages`
//! surfaces.

mod common;

use std::sync::atomic::Ordering;

use faucet_core::Source;
use faucet_source_grpc::{GrpcStream, GrpcStreamConfig, RpcKind};
use futures::StreamExt;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unary_fetch_all_returns_all_items() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "List",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 5 }))
    .records_path("$.items[*]");

    let stream = GrpcStream::new(config).unwrap();
    let records = stream.fetch_all().await.unwrap();

    assert_eq!(records.len(), 5);
    // Protobuf scalar defaults (id == 0) are omitted from the JSON output,
    // so assert on `name` which is always present.
    assert_eq!(records[0]["name"], "item-0");
    assert_eq!(records[1]["id"], 1);
    assert_eq!(records[4]["name"], "item-4");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_streaming_fetch_all_collects_all_events() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 7, "fail_after": 0 }))
    .rpc_kind(RpcKind::ServerStreaming);

    let stream = GrpcStream::new(config).unwrap();
    let records = stream.fetch_all().await.unwrap();

    assert_eq!(records.len(), 7);
    for (i, rec) in records.iter().enumerate() {
        assert_eq!(rec["payload"], format!("event-{i}"));
    }
    // A single attempt should be enough on the happy path.
    assert_eq!(server.tail_attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_streaming_stream_pages_yields_pages_of_batch_size() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 10, "fail_after": 0 }))
    .rpc_kind(RpcKind::ServerStreaming)
    .with_batch_size(3);

    let stream = GrpcStream::new(config).unwrap();
    let ctx = std::collections::HashMap::new();
    let mut pages = stream.stream_pages(&ctx, 3);
    let mut total = 0usize;
    let mut page_sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        page_sizes.push(page.records.len());
        total += page.records.len();
        // All server-streaming pages carry no bookmark — the source has no
        // native cursor and the test fixture has no resume token.
        assert!(page.bookmark.is_none());
    }
    assert_eq!(total, 10);
    // 3 + 3 + 3 + 1 trailing
    assert_eq!(page_sizes, vec![3, 3, 3, 1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_streaming_max_messages_caps_consumption() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 100, "fail_after": 0 }))
    .rpc_kind(RpcKind::ServerStreaming)
    .max_messages(4)
    .with_batch_size(0);

    let stream = GrpcStream::new(config).unwrap();
    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_streaming_reconnect_dedupes_replayed_prefix() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 5, "fail_after": 2 }))
    .rpc_kind(RpcKind::ServerStreaming)
    .reconnect_initial_backoff(std::time::Duration::from_millis(10))
    .reconnect_max_backoff(std::time::Duration::from_millis(20))
    .reconnect_max_attempts(3);

    let stream = GrpcStream::new(config).unwrap();
    let records = stream.fetch_all().await.unwrap();

    // First attempt yields events 0,1 then disconnects. The fixture replays
    // from message 0 on reconnect (count = 5 → events 0..4). With the default
    // `reconnect_replay_from_start = true`, the source skips the 2 already-
    // emitted messages on the reconnect, so each event is delivered exactly
    // once: 0,1,2,3,4 — no duplicates (#78/#23).
    assert_eq!(records.len(), 5);
    let payloads: Vec<&str> = records
        .iter()
        .map(|r| r["payload"].as_str().unwrap())
        .collect();
    assert_eq!(
        payloads,
        vec!["event-0", "event-1", "event-2", "event-3", "event-4"]
    );
    assert_eq!(server.tail_attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_streaming_reconnect_at_least_once_when_replay_disabled() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 5, "fail_after": 2 }))
    .rpc_kind(RpcKind::ServerStreaming)
    .reconnect_initial_backoff(std::time::Duration::from_millis(10))
    .reconnect_max_backoff(std::time::Duration::from_millis(20))
    .reconnect_max_attempts(3)
    // Opt out of dedup: emit every received message (at-least-once).
    .reconnect_replay_from_start(false);

    let stream = GrpcStream::new(config).unwrap();
    let records = stream.fetch_all().await.unwrap();

    // events 0,1 (attempt 1) + events 0,1,2,3,4 (replayed in full) = 7.
    assert_eq!(records.len(), 7);
    assert_eq!(records[0]["payload"], "event-0");
    assert_eq!(records[1]["payload"], "event-1");
    assert_eq!(records[2]["payload"], "event-0");
    assert_eq!(records[6]["payload"], "event-4");
    assert_eq!(server.tail_attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_streaming_terminate_on_error_propagates() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 5, "fail_after": 1 }))
    .rpc_kind(RpcKind::ServerStreaming)
    .terminate_on_error(true);

    let stream = GrpcStream::new(config).unwrap();
    let err = stream
        .fetch_all()
        .await
        .expect_err("should propagate error");
    let msg = format!("{err}");
    assert!(
        msg.contains("server-streaming") || msg.contains("simulated disconnect"),
        "unexpected error: {msg}"
    );
    // Exactly one attempt — no reconnect when terminate_on_error is true.
    assert_eq!(server.tail_attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_streaming_reconnect_max_attempts_surfaces_error() {
    // Server is bound but immediately shut down so every connect fails.
    let server = common::start_server().await;
    let endpoint = server.endpoint.clone();
    drop(server);

    let config = GrpcStreamConfig::new(
        &endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 1, "fail_after": 0 }))
    .rpc_kind(RpcKind::ServerStreaming)
    .reconnect_initial_backoff(std::time::Duration::from_millis(5))
    .reconnect_max_backoff(std::time::Duration::from_millis(10))
    .reconnect_max_attempts(2);

    let stream = GrpcStream::new(config).unwrap();
    let err = stream.fetch_all().await.expect_err("should give up");
    let msg = format!("{err}");
    assert!(
        msg.contains("reconnect_max_attempts"),
        "unexpected error: {msg}"
    );
}
