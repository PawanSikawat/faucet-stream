//! In-flight run registry. Tracks per-run cancellation tokens and the queue /
//! in-flight counters that drive backpressure (429), the `faucet_serve_runs_*`
//! gauges, `/readyz`, and the shutdown drain. A "queued" run is one that has been
//! spawned but has not yet acquired an execution permit.

use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

pub struct Registry {
    tokens: DashMap<String, CancellationToken>,
    queued: AtomicUsize,
    in_flight: AtomicUsize,
    max_queued: usize,
    drained: Notify,
}

impl Registry {
    pub fn new(max_queued: usize) -> Self {
        Self {
            tokens: DashMap::new(),
            queued: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            max_queued: max_queued.max(1),
            drained: Notify::new(),
        }
    }

    /// Reserve a queue slot. Returns `true` if a slot was reserved, or `false`
    /// if the queue is full. Atomic against concurrent submits via a CAS loop
    /// (`AtomicUsize::try_update`, stabilized in Rust 1.95).
    pub fn try_reserve(&self) -> bool {
        self.queued
            .try_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                (cur < self.max_queued).then_some(cur + 1)
            })
            .is_ok()
    }

    /// Release a slot reserved by `try_reserve` that will not be spawned
    /// (idempotency replay/conflict).
    pub fn release_reservation(&self) {
        self.queued.fetch_sub(1, Ordering::AcqRel);
    }

    pub fn register(&self, run_id: String, token: CancellationToken) {
        self.tokens.insert(run_id, token);
    }

    /// Queued → running: the run acquired its execution permit.
    pub fn mark_running(&self) {
        self.queued.fetch_sub(1, Ordering::AcqRel);
        self.in_flight.fetch_add(1, Ordering::AcqRel);
    }

    /// Running → terminal: drop the token and wake any shutdown drain waiter.
    pub fn mark_finished(&self, run_id: &str) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.tokens.remove(run_id);
        self.drained.notify_waiters();
    }

    /// Queued → terminal: a run was cancelled (or the server shut down) while
    /// still waiting for an execution permit, so it never became in-flight.
    /// Release its **queue** slot (not `in_flight`), drop the token, and wake any
    /// drain waiter. Distinct from [`Self::mark_finished`], which decrements
    /// `in_flight` (#146 R: a cancel on a queued run now takes effect at once).
    pub fn mark_queued_cancelled(&self, run_id: &str) {
        self.queued.fetch_sub(1, Ordering::AcqRel);
        self.tokens.remove(run_id);
        self.drained.notify_waiters();
    }

    /// Cancel a live run. Returns `true` if a live token existed.
    pub fn cancel(&self, run_id: &str) -> bool {
        if let Some(t) = self.tokens.get(run_id) {
            t.cancel();
            true
        } else {
            false
        }
    }

    pub fn queued(&self) -> usize {
        self.queued.load(Ordering::Acquire)
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    pub fn is_full(&self) -> bool {
        self.queued() >= self.max_queued
    }

    /// Resolve once no run is queued or in flight. Arms the notification *before*
    /// re-checking so a transition can't be missed.
    pub async fn wait_drained(&self) {
        loop {
            if self.queued() == 0 && self.in_flight() == 0 {
                return;
            }
            let notified = self.drained.notified();
            if self.queued() == 0 && self.in_flight() == 0 {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_respects_capacity() {
        let r = Registry::new(2);
        assert!(r.try_reserve());
        assert!(r.try_reserve());
        assert!(!r.try_reserve());
        assert!(r.is_full());
        r.release_reservation();
        assert!(r.try_reserve());
    }

    #[test]
    fn running_transition_moves_counters() {
        let r = Registry::new(4);
        r.try_reserve();
        assert_eq!(r.queued(), 1);
        r.mark_running();
        assert_eq!(r.queued(), 0);
        assert_eq!(r.in_flight(), 1);
        r.mark_finished("x");
        assert_eq!(r.in_flight(), 0);
    }

    #[test]
    fn cancel_reports_presence() {
        let r = Registry::new(4);
        let token = CancellationToken::new();
        r.register("run1".into(), token.clone());
        assert!(r.cancel("run1"));
        assert!(token.is_cancelled());
        assert!(!r.cancel("missing"));
    }

    #[test]
    fn is_not_idle_while_queued() {
        let r = Registry::new(4);
        r.try_reserve();
        // queued=1, in_flight=0 — must NOT be considered drained.
        assert_eq!(r.queued(), 1);
        assert_eq!(r.in_flight(), 0);
    }

    #[tokio::test]
    async fn wait_drained_returns_when_idle() {
        let r = Registry::new(4);
        // No in-flight work → resolves immediately.
        r.wait_drained().await;
    }
}
