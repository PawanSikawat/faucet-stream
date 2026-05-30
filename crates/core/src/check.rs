//! Preflight check types for `faucet doctor` (#126).
//!
//! A connector's `check()` returns a [`CheckReport`] of [`Probe`]s. Probe-level
//! failures are [`ProbeStatus::Fail`] inside an `Ok(report)`; an `Err` from
//! `check()` means "couldn't run any probe" and is rendered as a single failure.

use serde::Serialize;
use std::time::Duration;

/// Inputs a probe may need. The doctor command enforces `timeout` on the whole
/// `check()` call; connectors may also use it to bound their own client calls.
#[derive(Debug, Clone)]
pub struct CheckContext {
    /// Wall-clock budget for a single `check()` invocation.
    pub timeout: Duration,
}

impl Default for CheckContext {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
        }
    }
}

/// Outcome of a single probe.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ProbeStatus {
    /// The probe succeeded.
    Pass,
    /// The probe ran and the target is unhealthy / unreachable / misconfigured.
    Fail { reason: String },
    /// The probe was not applicable (no check implemented, optional target absent).
    Skip { reason: String },
}

/// One named probe within a [`CheckReport`] (e.g. `"read"`, `"auth"`,
/// `"network"`, `"permissions"`, `"schema"`, `"io"`, `"sentinel"`).
#[derive(Debug, Clone, Serialize)]
pub struct Probe {
    /// Short, stable probe name.
    pub name: &'static str,
    /// The outcome; serialized inline as `status` (+ `reason` on fail/skip).
    #[serde(flatten)]
    pub status: ProbeStatus,
    /// Wall-clock time the probe took.
    pub elapsed_ms: u64,
    /// Remediation hint shown on failure. Must never contain secrets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

impl Probe {
    /// A passing probe.
    pub fn pass(name: &'static str, elapsed: Duration) -> Self {
        Self {
            name,
            status: ProbeStatus::Pass,
            elapsed_ms: elapsed.as_millis() as u64,
            hint: None,
        }
    }

    /// A failing probe with a reason and no hint.
    pub fn fail(name: &'static str, elapsed: Duration, reason: impl Into<String>) -> Self {
        Self {
            name,
            status: ProbeStatus::Fail {
                reason: reason.into(),
            },
            elapsed_ms: elapsed.as_millis() as u64,
            hint: None,
        }
    }

    /// A failing probe with a reason and a remediation hint.
    pub fn fail_hint(
        name: &'static str,
        elapsed: Duration,
        reason: impl Into<String>,
        hint: impl Into<String>,
    ) -> Self {
        Self {
            name,
            status: ProbeStatus::Fail {
                reason: reason.into(),
            },
            elapsed_ms: elapsed.as_millis() as u64,
            hint: Some(hint.into()),
        }
    }

    /// A skipped (not-applicable) probe.
    pub fn skip(name: &'static str, reason: impl Into<String>) -> Self {
        Self {
            name,
            status: ProbeStatus::Skip {
                reason: reason.into(),
            },
            elapsed_ms: 0,
            hint: None,
        }
    }
}

/// A connector's full preflight report.
#[derive(Debug, Clone, Serialize, Default)]
pub struct CheckReport {
    /// The probes run, in order.
    pub probes: Vec<Probe>,
}

impl CheckReport {
    /// A report with a single probe.
    pub fn single(probe: Probe) -> Self {
        Self {
            probes: vec![probe],
        }
    }

    /// The default report for connectors that don't implement a probe.
    pub fn not_implemented() -> Self {
        Self::single(Probe::skip("check", "no check implemented"))
    }

    /// Number of `Fail` probes (used to compute the doctor exit code).
    pub fn failed_count(&self) -> usize {
        self.probes
            .iter()
            .filter(|p| matches!(p.status, ProbeStatus::Fail { .. }))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn pass_and_fail_constructors_set_status_and_elapsed() {
        let p = Probe::pass("read", Duration::from_millis(42));
        assert_eq!(p.name, "read");
        assert!(matches!(p.status, ProbeStatus::Pass));
        assert_eq!(p.elapsed_ms, 42);
        assert!(p.hint.is_none());

        let f = Probe::fail_hint("auth", Duration::from_millis(5), "bad token", "set TOKEN");
        assert!(matches!(f.status, ProbeStatus::Fail { .. }));
        assert_eq!(f.hint.as_deref(), Some("set TOKEN"));
    }

    #[test]
    fn not_implemented_is_a_single_skip() {
        let r = CheckReport::not_implemented();
        assert_eq!(r.probes.len(), 1);
        assert!(matches!(r.probes[0].status, ProbeStatus::Skip { .. }));
        assert_eq!(r.failed_count(), 0);
    }

    #[test]
    fn failed_count_counts_only_fail() {
        let r = CheckReport {
            probes: vec![
                Probe::pass("a", Duration::ZERO),
                Probe::fail("b", Duration::ZERO, "x"),
                Probe::skip("c", "n/a"),
                Probe::fail("d", Duration::ZERO, "y"),
            ],
        };
        assert_eq!(r.failed_count(), 2);
    }

    #[test]
    fn probe_serializes_status_inline() {
        let p = Probe::fail("auth", Duration::from_millis(1), "nope");
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["name"], "auth");
        assert_eq!(v["status"], "fail");
        assert_eq!(v["reason"], "nope");
        assert_eq!(v["elapsed_ms"], 1);
    }
}
