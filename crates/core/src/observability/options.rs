//! Options struct passed to `run_stream`. Replaces the prior positional
//! argument list so future observability additions don't keep breaking the
//! function signature.

use crate::dlq::DlqConfig;
use crate::state::StateStore;
use std::sync::Arc;

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

    #[cfg(feature = "quality")]
    pub fn with_quality(
        mut self,
        quality: std::sync::Arc<crate::quality::CompiledQuality>,
    ) -> Self {
        self.quality = Some(quality);
        self
    }
}
