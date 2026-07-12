//! `faucet-conformance` Tier-1 battery for the GCS source.
//!
//! Check 1 (config-schema validity) is pure and offline and always runs.
//!
//! Check 2 (bounded-memory streaming) is `#[ignore]`d for the same reason the
//! rest of this crate's integration suite is: `fake-gcs-server` only speaks the
//! REST API, but the GCS source reads via the gRPC storage client, so the
//! emulator cannot actually serve `stream_pages` reads. The check is wired
//! against a real GCS-compatible gRPC backend and matches the existing
//! `integration.rs` emulator setup; run it with `cargo test -- --ignored`
//! against a live backend.

#![cfg(not(target_os = "windows"))]

use faucet_conformance::{assert_config_schema_valid_value, assert_errors_not_panics};
use faucet_source_gcs::{GcsCredentials, GcsSource, GcsSourceConfig};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(GcsSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-gcs");
}

// ── Check 2: bounded-memory streaming (live gRPC backend, ignored) ──────────

/// Spawn `fake-gcs-server` and return `(host_url, bucket_name)`.
/// Returns `None` when Docker is unavailable so tests skip cleanly.
async fn spawn_fake_gcs() -> Option<(String, String)> {
    let image = GenericImage::new("fsouza/fake-gcs-server", "latest")
        .with_exposed_port(4443.tcp())
        .with_wait_for(WaitFor::message_on_stderr("server started at"))
        .with_cmd(vec![
            "-scheme=http".to_string(),
            "-public-host=0.0.0.0:4443".to_string(),
        ]);
    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping: Docker not available ({e})");
            return None;
        }
    };
    let port = container.get_host_port_ipv4(4443).await.ok()?;
    let host = format!("http://127.0.0.1:{port}");
    let bucket = "faucet-conformance".to_string();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{host}/storage/v1/b"))
        .json(&serde_json::json!({"name": bucket}))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() && resp.status() != reqwest::StatusCode::CONFLICT {
        eprintln!("Skipping: could not create bucket ({})", resp.status());
        return None;
    }

    // Container lifetime tied to process exit.
    std::mem::forget(container);
    Some((host, bucket))
}

/// Upload an object via fake-gcs-server's REST surface.
async fn seed_object(host: &str, bucket: &str, name: &str, body: &str, content_type: &str) {
    let client = reqwest::Client::new();
    let url = format!(
        "{host}/upload/storage/v1/b/{bucket}/o?uploadType=media&name={}",
        urlencoding::encode(name)
    );
    client
        .post(url)
        .header("Content-Type", content_type)
        .body(body.to_string())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

/// Build a JSONL body with records `{"id": i}` for `i = 1..=n`.
fn jsonl_body(n: i64) -> String {
    let mut out = String::new();
    for i in 1..=n {
        out.push_str(&format!("{{\"id\":{i}}}\n"));
    }
    out
}

#[tokio::test]
#[ignore = "requires a real GCS-compatible gRPC backend; fake-gcs-server only speaks REST. Run with `cargo test -- --ignored` against a live backend."]
async fn conformance_bounded_memory() {
    let Some((host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    seed_object(
        &host,
        &bucket,
        "data/events.jsonl",
        &jsonl_body(5_000),
        "application/x-ndjson",
    )
    .await;

    let config = GcsSourceConfig::new(&bucket)
        .prefix("data/")
        .auth(GcsCredentials::Anonymous)
        .storage_host(&host)
        .with_batch_size(250);
    let source = GcsSource::new(config).await.unwrap();

    faucet_conformance::assert_bounded_memory(&source, 250, 5_000).await;
}

// ── Check 6: errors, not panics (no container) ──────────────────────────────

/// Point the source at an unreachable GCS storage host (`http://127.0.0.1:1`,
/// which refuses connections immediately) with anonymous credentials. `new()`
/// stays lazy — building the gRPC storage clients with `Anonymous` creds + an
/// endpoint override does not perform I/O (see `faucet-common-gcs` tests) — so
/// no container is needed. The first list/get RPC fails with a typed
/// `FaucetError` on both the `fetch_all` and `stream_pages` paths, never a
/// panic.
///
/// Unlike Check 2 (which is `#[ignore]`d because the REST emulator cannot serve
/// the gRPC read path), Check 6 only needs the *failure* path, which an
/// unreachable host reproduces deterministically — so it runs unconditionally.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let config = GcsSourceConfig::new("does-not-exist")
        .prefix("data/")
        .auth(GcsCredentials::Anonymous)
        .storage_host("http://127.0.0.1:1")
        .with_batch_size(250);
    let source = GcsSource::new(config).await.expect("source builds lazily");
    assert_errors_not_panics(&source).await;
}
