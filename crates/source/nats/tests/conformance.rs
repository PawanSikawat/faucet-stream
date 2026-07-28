//! `faucet-conformance` Tier-1 battery for the NATS source.
//!
//! - **Check 1** (`conformance_config_schema_valid`) — pure/offline, MUST pass.
//! - **Check 6** (`conformance_errors_not_panics`) — points at an unreachable
//!   server (`nats://127.0.0.1:1`); connect fails on the first poll, so this
//!   runs offline and MUST pass.
//! - **Check 2** (`conformance_bounded_memory`) — boots a real NATS server via
//!   `testcontainers-modules` (Docker); publishes N messages, then drains with
//!   `max_messages = N` so the bounded-memory check can assert `seen == total`.
//!
//! Bookmark/idempotency checks (3/4/5) do not apply: core NATS is
//! fire-and-forget (no bookmark) and 4/5 are sink-only.

use faucet_conformance::{assert_config_schema_valid_value, assert_errors_not_panics};
use faucet_source_nats::{NatsSource, NatsSourceConfig};

// ── Check 1: config schema (offline) ────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(NatsSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-nats");
}

// ── Check 6: errors, not panics (offline — unreachable server) ───────────────

#[tokio::test]
async fn conformance_errors_not_panics() {
    let mut cfg = NatsSourceConfig::new("events.>");
    cfg.connection.servers = vec!["nats://127.0.0.1:1".into()];
    // A short terminator keeps the run bounded even on the (unreachable) happy
    // path; the connect failure surfaces before it ever matters.
    cfg.idle_timeout_secs = Some(1);
    let source = NatsSource::new(cfg)
        .await
        .expect("lazy construction succeeds");
    assert_errors_not_panics(&source).await;
}

// ── Check 2: bounded-memory streaming (Docker) ───────────────────────────────

#[cfg(test)]
mod docker {
    use super::*;
    use faucet_source_nats::Source;
    use futures::StreamExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::nats::Nats;

    async fn start_nats() -> (testcontainers::ContainerAsync<Nats>, String) {
        let container = Nats::default().start().await.expect("nats container start");
        let host = container.get_host().await.expect("nats host");
        let port = container.get_host_port_ipv4(4222).await.expect("nats port");
        (container, format!("nats://{host}:{port}"))
    }

    async fn publish_json(server: &str, subject: &str, count: usize) {
        let client = async_nats::connect(server).await.expect("connect");
        for i in 1..=count {
            let payload = format!(r#"{{"id":{i}}}"#);
            client
                .publish(subject.to_string(), payload.into_bytes().into())
                .await
                .expect("publish");
        }
        client.flush().await.expect("flush");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn conformance_bounded_memory() {
        let (_container, server) = start_nats().await;
        let subject = "conformance.bounded";
        const N: usize = 5_000;

        // Subscribe first (core NATS drops messages published before a
        // subscription exists), then publish, then drain.
        let mut cfg = NatsSourceConfig::new(subject);
        cfg.connection.servers = vec![server.clone()];
        cfg.max_messages = Some(N);
        cfg.idle_timeout_secs = Some(30);
        cfg.batch_size = 250;
        let source = NatsSource::new(cfg).await.expect("source new");

        // Drive the drain concurrently with the publisher: start streaming so the
        // subscription is live, then publish the N messages.
        let ctx = std::collections::HashMap::new();
        let publisher = {
            let server = server.clone();
            tokio::spawn(async move {
                // Small delay so the subscription is established first.
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                publish_json(&server, subject, N).await;
            })
        };

        let mut stream = source.stream_pages(&ctx, 250);
        let mut seen = 0usize;
        let mut peak = 0usize;
        while let Some(page) = stream.next().await {
            let page = page.expect("page");
            peak = peak.max(page.records.len());
            seen += page.records.len();
        }
        publisher.await.expect("publisher");

        assert_eq!(seen, N, "streamed {seen}, expected {N}");
        assert!(peak <= 250, "peak page {peak} exceeds batch_size");
        assert!(peak < N, "buffered everything into one page");
    }
}
