//! Event transports.

use async_trait::async_trait;
use faucet_core::FaucetError;

pub mod file;
pub mod http;
#[cfg(feature = "transport-kafka")]
pub mod kafka;

/// A sink for serialized OpenLineage events. Implementations send one event
/// (already serialized to JSON bytes) per call. Errors are surfaced to the
/// emitter, which logs + drops them — they never fail the pipeline run.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, event_json: Vec<u8>) -> Result<(), FaucetError>;
}
