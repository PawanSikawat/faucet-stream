//! `faucet-conformance` battery for the webhook source.
//!
//! Check 1: the connector's config JSON Schema is a well-formed schema value
//!          (pure, offline — always runs).
//!
//! Check 2 (bounded memory) is intentionally NOT run here: the webhook source
//! is buffer-shaped — it runs a temporary HTTP server that accumulates incoming
//! POSTs into an in-memory buffer during the receive window and then uses the
//! DEFAULT `Source::stream_pages` chunking impl (it does not override it). That
//! default chunking's bounded-memory behaviour is covered by faucet-core's own
//! tests, and driving the receiver end-to-end would require a concurrent HTTP
//! client racing the server's bind/receive window on a fixed port — not cleanly
//! or reliably expressible through the `assert_bounded_memory` harness (which
//! owns the `stream_pages` drive). Check 1 (config schema) applies here.
//!
//! Check 6 (errors, not panics) is expressible cleanly: the webhook source
//! binds a `TcpListener` to `listen_addr` at read time, so an unbindable
//! address deterministically drives the failure path.

use faucet_conformance::{assert_config_schema_valid_value, assert_errors_not_panics};
use faucet_source_webhook::{WebhookSource, WebhookSourceConfig};

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(WebhookSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-webhook");
}

// ── Check 6: errors, not panics ──────────────────────────────────────────────

/// Configure the source with an unbindable `listen_addr`. `new()` is lazy — it
/// only stores the config; the bind happens when the receive window opens.
/// A malformed address string fails `TcpListener::bind` (address resolution),
/// surfacing a typed `FaucetError::Config` on both the `fetch_all` and
/// `stream_pages` paths, never a panic — with no port reservation and no race,
/// so the failure is fully deterministic on every platform.
#[tokio::test]
async fn conformance_errors_not_panics() {
    let source =
        WebhookSource::new(WebhookSourceConfig::new().listen_addr("999.999.999.999:99999"));
    assert_errors_not_panics(&source).await;
}
