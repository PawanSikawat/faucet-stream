//! Options struct passed to `run_stream`. Replaces the prior positional
//! argument list so future observability additions don't keep breaking the
//! function signature.

use crate::dlq::DlqConfig;
use crate::state::StateStore;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Default, Clone)]
pub struct RunStreamOptions {
    pub state_store: Option<Arc<dyn StateStore>>,
    pub state_key: Option<String>,
    pub pipeline_name: Option<String>,
    pub row: Option<String>,
    pub run_id: Option<String>,
    pub dlq: Option<DlqConfig>,
    #[cfg(feature = "quality")]
    pub quality: Option<std::sync::Arc<crate::quality::CompiledQuality>>,
    /// Adaptive batch-size controller config; `None` (or `enabled = false`)
    /// leaves the per-page write path unchanged.
    pub adaptive: Option<crate::adaptive::AdaptiveBatchConfig>,
    /// Cooperative cancellation. When set and cancelled mid-run, the streaming
    /// loop stops polling new pages, **flushes the sinks** (so a buffered sink
    /// like Parquet writes its footer / completes its upload rather than
    /// orphaning the file), and returns the partial result. Without this, a
    /// dropped run future loses everything written-but-unflushed (#146 H16).
    pub cancel: Option<CancellationToken>,
}

impl RunStreamOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_state(mut self, store: Arc<dyn StateStore>, key: impl Into<String>) -> Self {
        self.state_store = Some(store);
        self.state_key = Some(key.into());
        self
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.pipeline_name = Some(name.into());
        self
    }

    pub fn with_row(mut self, row: impl Into<String>) -> Self {
        self.row = Some(row.into());
        self
    }

    pub fn with_run_id(mut self, id: impl Into<String>) -> Self {
        self.run_id = Some(id.into());
        self
    }

    pub fn with_dlq(mut self, dlq: DlqConfig) -> Self {
        self.dlq = Some(dlq);
        self
    }

    /// Attach a cancellation token for cooperative, flush-completing cancel.
    pub fn with_cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = Some(cancel);
        self
    }

    /// Attach an adaptive batch-size controller config.
    pub fn with_adaptive(mut self, cfg: crate::adaptive::AdaptiveBatchConfig) -> Self {
        self.adaptive = Some(cfg);
        self
    }

    #[cfg(feature = "quality")]
    pub fn with_quality(
        mut self,
        quality: std::sync::Arc<crate::quality::CompiledQuality>,
    ) -> Self {
        self.quality = Some(quality);
        self
    }
}
