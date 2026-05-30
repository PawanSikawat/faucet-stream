//! Adaptive batch sizing — an AIMD controller that auto-tunes the effective
//! write batch size per pipeline row from observed sink latency + error rate.
//! Pure logic (no I/O); `run_stream` feeds it observations and emits metrics.
//! See `docs/superpowers/specs/2026-05-31-adaptive-batch-sizing-design.md`.

use crate::error::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_controller() -> String { "aimd".to_string() }
fn default_min() -> usize { 100 }
fn default_max() -> usize { 50_000 }
fn default_increase_step() -> usize { 250 }
fn default_decrease_factor() -> f64 { 0.5 }
fn default_cooldown_batches() -> usize { 5 }
fn default_latency_window() -> usize { 10 }
fn default_error_threshold() -> f64 { 0.01 }
fn default_true() -> bool { true }
fn default_log_every() -> usize { 50 }

/// Configuration for the adaptive batch-size controller. Lives under
/// `execution.adaptive_batch_size`. Default `enabled = false` (opt-in); when
/// disabled the pipeline writes each page exactly as before.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AdaptiveBatchConfig {
    /// Master switch. Default `false`.
    #[serde(default)]
    pub enabled: bool,
    /// Controller algorithm. Only `"aimd"` is implemented in v1.
    #[serde(default = "default_controller")]
    pub controller: String,
    /// Lower bound on the effective batch size.
    #[serde(default = "default_min")]
    pub min: usize,
    /// Upper bound. Under within-page reslicing the effective ceiling is
    /// `min(max, page_len)`; values above the source page size are inert.
    #[serde(default = "default_max")]
    pub max: usize,
    /// Additive growth per clean+fast batch.
    #[serde(default = "default_increase_step")]
    pub increase_step: usize,
    /// Multiplicative shrink factor on error / high latency (0 < f < 1).
    #[serde(default = "default_decrease_factor")]
    pub decrease_factor: f64,
    /// Batches to wait after a shrink before allowing growth.
    #[serde(default = "default_cooldown_batches")]
    pub cooldown_batches: usize,
    /// Optional latency target in ms. `None` = react to errors only.
    #[serde(default)]
    pub target_latency_ms: Option<u64>,
    /// Rolling window size for the p50 batch-write latency.
    #[serde(default = "default_latency_window")]
    pub latency_window: usize,
    /// Per-batch error rate above which the controller shrinks.
    #[serde(default = "default_error_threshold")]
    pub error_threshold: f64,
    /// Never grow past the source page size. v1 honors only `true`; `false`
    /// logs a one-shot warning and behaves as `true` (cross-page buffering is
    /// a future enhancement).
    #[serde(default = "default_true")]
    pub respect_source_max: bool,
    /// Emit a `tracing::info!` summary every N adjustments.
    #[serde(default = "default_log_every")]
    pub log_every: usize,
}

impl AdaptiveBatchConfig {
    /// Fail-fast validation, surfaced as `FaucetError::Config` at config load.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.controller != "aimd" {
            return Err(FaucetError::Config(format!(
                "adaptive_batch_size.controller '{}' is not supported (only 'aimd')",
                self.controller
            )));
        }
        if self.min < 1 {
            return Err(FaucetError::Config(
                "adaptive_batch_size.min must be >= 1".into(),
            ));
        }
        if self.min > self.max {
            return Err(FaucetError::Config(format!(
                "adaptive_batch_size.min ({}) must be <= max ({})",
                self.min, self.max
            )));
        }
        if !(self.decrease_factor > 0.0 && self.decrease_factor < 1.0) {
            return Err(FaucetError::Config(
                "adaptive_batch_size.decrease_factor must be in (0, 1)".into(),
            ));
        }
        if self.increase_step < 1 {
            return Err(FaucetError::Config(
                "adaptive_batch_size.increase_step must be >= 1".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.error_threshold) {
            return Err(FaucetError::Config(
                "adaptive_batch_size.error_threshold must be in [0, 1]".into(),
            ));
        }
        if self.latency_window < 1 {
            return Err(FaucetError::Config(
                "adaptive_batch_size.latency_window must be >= 1".into(),
            ));
        }
        if let Some(t) = self.target_latency_ms
            && t == 0
        {
            return Err(FaucetError::Config(
                "adaptive_batch_size.target_latency_ms must be > 0 when set".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    fn valid() -> AdaptiveBatchConfig {
        serde_json::from_value(serde_json::json!({"enabled": true})).unwrap()
    }

    #[test]
    fn defaults_are_sane_and_valid() {
        let c = valid();
        assert_eq!(c.controller, "aimd");
        assert_eq!(c.min, 100);
        assert_eq!(c.max, 50_000);
        assert!(c.respect_source_max);
        assert!(c.target_latency_ms.is_none());
        c.validate().unwrap();
    }

    #[test]
    fn rejects_unknown_controller() {
        let mut c = valid();
        c.controller = "pid".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_min_gt_max_and_zero_min() {
        let mut c = valid();
        c.min = 10;
        c.max = 5;
        assert!(c.validate().is_err());
        let mut c = valid();
        c.min = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_out_of_range_factors() {
        let mut c = valid();
        c.decrease_factor = 1.5;
        assert!(c.validate().is_err());
        let mut c = valid();
        c.error_threshold = 2.0;
        assert!(c.validate().is_err());
        let mut c = valid();
        c.increase_step = 0;
        assert!(c.validate().is_err());
        let mut c = valid();
        c.target_latency_ms = Some(0);
        assert!(c.validate().is_err());
        // decrease_factor is an *exclusive* (0, 1) range — both bounds invalid.
        let mut c = valid();
        c.decrease_factor = 0.0;
        assert!(c.validate().is_err());
        let mut c = valid();
        c.decrease_factor = 1.0;
        assert!(c.validate().is_err());
        // latency_window must be >= 1.
        let mut c = valid();
        c.latency_window = 0;
        assert!(c.validate().is_err());
    }
}
