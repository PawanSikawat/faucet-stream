//! Runs the reusable `faucet-conformance` battery against the real REST source.
//!
//! - check 1 `assert_config_schema_valid`
//! - check 6 `assert_errors_not_panics` — an unreachable endpoint surfaces a
//!   typed `FaucetError` (connection refused) rather than panicking.
//!
//! check 2 (bounded-memory streaming) and the incremental resume path (check 3)
//! are covered in depth by this crate's dedicated `stream_test.rs`,
//! `pagination_test.rs`, and `state_resume_test.rs` against a live wiremock
//! server; the battery here adds the uniform schema + error-handling contract.

use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};

fn unreachable_source() -> RestStream {
    // Port 1 refuses connections immediately on all platforms — a fast,
    // deterministic "unreachable endpoint" with no external dependency.
    RestStream::new(
        RestStreamConfig::new("http://127.0.0.1:1", "/items").pagination(PaginationStyle::None),
    )
    .expect("rest stream builds from a valid config")
}

#[test]
fn conformance_config_schema_valid() {
    faucet_conformance::assert_config_schema_valid(&unreachable_source());
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    faucet_conformance::assert_errors_not_panics(&unreachable_source()).await;
}
