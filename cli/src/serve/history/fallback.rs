//! Degradation wrapper around a persistent run-history backend (Phase 5 of
//! #127). While the primary (Postgres/SQLite) backend is healthy, every call
//! goes to it. The first time a call errors — or if the backend could not be
//! reached at startup — the wrapper flips to **degraded**: it logs once, sets
//! the `faucet_serve_history_degraded` gauge, surfaces `503` on `/readyz` via
//! [`RunHistory::degraded`], and serves all subsequent calls from an in-memory
//! backend so the server stays up (spec §11). Data already in the primary is not
//! migrated — degraded mode is a stay-alive fallback, not a replica.

use super::memory::MemoryHistory;
use super::{Claim, DeleteOutcome, HistoryError, ListFilter, ListPage, RunHistory, RunRecord};
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub struct FallbackHistory {
    /// Persistent backend; `None` when it was unreachable at startup.
    primary: Option<Box<dyn RunHistory>>,
    fallback: MemoryHistory,
    degraded: AtomicBool,
    /// `"postgres"` / `"sqlite"` — for log + metric context.
    label: &'static str,
}

impl FallbackHistory {
    /// Wrap a healthy primary backend.
    pub fn healthy(
        primary: Box<dyn RunHistory>,
        idem_retention: Duration,
        label: &'static str,
    ) -> Self {
        Self {
            primary: Some(primary),
            fallback: MemoryHistory::new(idem_retention),
            degraded: AtomicBool::new(false),
            label,
        }
    }

    /// Start already degraded: the primary backend was unreachable at startup.
    pub fn degraded_at_startup(idem_retention: Duration, label: &'static str) -> Self {
        crate::serve::metrics::set_history_degraded(true);
        Self {
            primary: None,
            fallback: MemoryHistory::new(idem_retention),
            degraded: AtomicBool::new(true),
            label,
        }
    }

    fn is_degraded(&self) -> bool {
        self.primary.is_none() || self.degraded.load(Ordering::Acquire)
    }

    /// Record a primary-backend failure and flip to degraded (log + metric once).
    fn trip(&self, err: &HistoryError) {
        if !self.degraded.swap(true, Ordering::AcqRel) {
            tracing::error!(
                backend = self.label,
                error = %err,
                "run-history backend failed; falling back to in-memory store (DEGRADED — \
                 persisted run records are no longer served; /readyz now reports 503)"
            );
            crate::serve::metrics::set_history_degraded(true);
        }
    }
}

/// Run `$call` against the primary backend; on the first error, trip into
/// degraded mode and re-run it against the in-memory fallback. Once degraded,
/// skip the primary entirely.
macro_rules! via {
    ($self:ident, $primary:ident => $pcall:expr, $fb:ident => $fcall:expr) => {{
        if !$self.is_degraded()
            && let Some($primary) = $self.primary.as_ref()
        {
            match $pcall.await {
                Ok(v) => return Ok(v),
                Err(e) => $self.trip(&e),
            }
        }
        let $fb = &$self.fallback;
        $fcall.await
    }};
}

#[async_trait]
impl RunHistory for FallbackHistory {
    async fn claim_idempotency(
        &self,
        key: &str,
        fingerprint: &str,
        run_id: &str,
        window: Duration,
    ) -> Result<Claim, HistoryError> {
        via!(self,
            p => p.claim_idempotency(key, fingerprint, run_id, window),
            f => f.claim_idempotency(key, fingerprint, run_id, window))
    }

    async fn upsert(&self, rec: &RunRecord) -> Result<(), HistoryError> {
        via!(self, p => p.upsert(rec), f => f.upsert(rec))
    }

    async fn get(&self, id: &str) -> Result<Option<RunRecord>, HistoryError> {
        via!(self, p => p.get(id), f => f.get(id))
    }

    async fn list(&self, filter: &ListFilter) -> Result<ListPage, HistoryError> {
        via!(self, p => p.list(filter), f => f.list(filter))
    }

    async fn delete(&self, id: &str) -> Result<DeleteOutcome, HistoryError> {
        via!(self, p => p.delete(id), f => f.delete(id))
    }

    async fn purge_expired(&self, retain_for: Duration) -> Result<usize, HistoryError> {
        via!(self, p => p.purge_expired(retain_for), f => f.purge_expired(retain_for))
    }

    async fn recover_orphans(&self) -> Result<usize, HistoryError> {
        via!(self, p => p.recover_orphans(), f => f.recover_orphans())
    }

    fn degraded(&self) -> bool {
        self.is_degraded()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::RunStatus;

    /// A primary backend whose every call fails — drives the degrade path.
    struct AlwaysFail;

    #[async_trait]
    impl RunHistory for AlwaysFail {
        async fn claim_idempotency(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: Duration,
        ) -> Result<Claim, HistoryError> {
            Err(HistoryError::Backend("down".into()))
        }
        async fn upsert(&self, _: &RunRecord) -> Result<(), HistoryError> {
            Err(HistoryError::Backend("down".into()))
        }
        async fn get(&self, _: &str) -> Result<Option<RunRecord>, HistoryError> {
            Err(HistoryError::Backend("down".into()))
        }
        async fn list(&self, _: &ListFilter) -> Result<ListPage, HistoryError> {
            Err(HistoryError::Backend("down".into()))
        }
        async fn delete(&self, _: &str) -> Result<DeleteOutcome, HistoryError> {
            Err(HistoryError::Backend("down".into()))
        }
        async fn purge_expired(&self, _: Duration) -> Result<usize, HistoryError> {
            Err(HistoryError::Backend("down".into()))
        }
        async fn recover_orphans(&self) -> Result<usize, HistoryError> {
            Err(HistoryError::Backend("down".into()))
        }
        fn degraded(&self) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn trips_to_fallback_on_primary_error_and_serves_from_memory() {
        let fb = FallbackHistory::healthy(Box::new(AlwaysFail), Duration::from_secs(60), "test");
        assert!(!fb.degraded(), "starts healthy");

        // First write fails on the primary, trips degraded, lands in memory.
        let rec = RunRecord::queued(
            "r1".into(),
            None,
            Default::default(),
            None,
            chrono::Utc::now(),
        );
        fb.upsert(&rec).await.unwrap();
        assert!(fb.degraded(), "primary error must flip degraded");

        // Subsequent reads are served from the in-memory fallback.
        let got = fb.get("r1").await.unwrap();
        assert_eq!(got.unwrap().run_id, "r1");
    }

    #[tokio::test]
    async fn degraded_at_startup_uses_memory_only() {
        let fb = FallbackHistory::degraded_at_startup(Duration::from_secs(60), "test");
        assert!(fb.degraded());
        let mut rec = RunRecord::queued(
            "r2".into(),
            None,
            Default::default(),
            None,
            chrono::Utc::now(),
        );
        rec.status = RunStatus::Completed;
        rec.finished_at = Some(chrono::Utc::now());
        fb.upsert(&rec).await.unwrap();
        assert_eq!(
            fb.get("r2").await.unwrap().unwrap().status,
            RunStatus::Completed
        );
        assert_eq!(fb.delete("r2").await.unwrap(), DeleteOutcome::Deleted);
    }
}
