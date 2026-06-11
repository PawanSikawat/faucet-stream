//! Clustered execution (#197, Mode A): when `--cluster` is set, every instance
//! runs a claim loop that pulls `Pending` runs from the shared SQL history DB,
//! so submissions pull-balance across instances and a crashed instance's runs
//! are re-run by a survivor. Inert unless enabled.

use std::time::Duration;

/// Validated cluster settings, derived from `--cluster*` args.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub enabled: bool,
    /// Claim-loop poll interval (also the cross-instance cancel-propagation lag).
    pub poll: Duration,
    /// Max failover re-runs before an orphan is marked Failed (poison).
    pub max_attempts: u32,
}

impl ClusterConfig {
    /// A disabled cluster (single-instance default).
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            poll: Duration::from_secs(2),
            max_attempts: 3,
        }
    }
}
