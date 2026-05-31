//! Per-run log capture for SSE streaming (`GET /v1/runs/{id}/logs`, spec §12).
//!
//! [`RunLogLayer`] is a `tracing` `Layer` added to serve's global subscriber. It
//! tags every span that carries a `serve_run_id` field (the
//! `faucet.serve.run` span each run executes inside — see `runner.rs`) and, for
//! every event in such a span's scope, formats a redacted line and pushes it into
//! that run's [`RunBuffer`]: a bounded ring (for backfill) plus a `broadcast`
//! channel (for the live tail). The `/logs` handler replays the ring, then
//! streams the live tail via [`log_events`].
//!
//! **Ephemeral lifecycle.** Buffers live while a run is active plus a short drain
//! window ([`LOG_DRAIN`]) for late fetchers, then are dropped regardless of
//! `--retain-terminal-runs-secs` — only `RunRecord` metadata honours that
//! retention. Bulk/historic logs belong in the centralized tracing sink.

use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::broadcast;

/// Per-run ring-buffer capacity (lines). Past this the oldest line is evicted and
/// late `/logs` subscribers see a `truncated` event.
pub const RING_CAPACITY: usize = 10_000;

/// Live-tail broadcast channel depth. A `/logs` reader that falls this far behind
/// gets a `truncated` event rather than blocking producers.
pub const BROADCAST_CAPACITY: usize = 1024;

/// How long a run's log buffer survives after the run reaches a terminal state,
/// so a late `/logs` fetcher can still replay it. Independent of run-record
/// retention (spec §12).
pub const LOG_DRAIN: Duration = Duration::from_secs(60);

/// A single captured log line, tagged with a monotonic sequence number so a late
/// `/logs` subscriber can de-duplicate ring backfill against the live tail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogLine {
    pub seq: u64,
    pub line: String,
}

/// A message on a run's live-tail broadcast channel.
#[derive(Clone, Debug)]
pub enum LogMsg {
    /// A newly captured log line.
    Line(LogLine),
    /// The run reached a terminal state; no further lines will arrive.
    End,
}

/// One run's bounded log buffer: a ring for backfill + a broadcast for live tail.
struct RunBuffer {
    ring: Mutex<VecDeque<LogLine>>,
    seq: AtomicU64,
    tx: broadcast::Sender<LogMsg>,
    ended: AtomicBool,
}

impl RunBuffer {
    fn new() -> Self {
        let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            ring: Mutex::new(VecDeque::with_capacity(64)),
            seq: AtomicU64::new(0),
            tx,
            ended: AtomicBool::new(false),
        }
    }

    /// Append a line: assign a sequence, push to the ring (evicting the oldest
    /// past the cap), and best-effort broadcast to live subscribers.
    fn push(&self, line: String) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let entry = LogLine { seq, line };
        {
            let mut ring = self.ring.lock().expect("log ring poisoned");
            if ring.len() == RING_CAPACITY {
                ring.pop_front();
            }
            ring.push_back(entry.clone());
        }
        // No live subscribers → Err; the ring still holds the line for backfill.
        let _ = self.tx.send(LogMsg::Line(entry));
    }

    fn snapshot(&self) -> Vec<LogLine> {
        self.ring
            .lock()
            .expect("log ring poisoned")
            .iter()
            .cloned()
            .collect()
    }

    /// Mark the run terminal and notify live subscribers. `ended` is stored
    /// **before** the broadcast so a reader that misses the `End` message always
    /// observes `is_ended() == true` (see [`LogHub::reader`]).
    fn finish(&self) {
        self.ended.store(true, Ordering::SeqCst);
        let _ = self.tx.send(LogMsg::End);
    }
}

/// Shared, cheaply-cloneable registry of per-run log buffers. One instance is
/// created at subscriber install and shared between [`RunLogLayer`] and the
/// `/logs` handler via `ServerState`.
#[derive(Clone, Default)]
pub struct LogHub {
    inner: Arc<DashMap<String, Arc<RunBuffer>>>,
}

impl LogHub {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get-or-create the buffer for a run (lazily, on first captured event).
    fn buffer(&self, run_id: &str) -> Arc<RunBuffer> {
        if let Some(b) = self.inner.get(run_id) {
            return Arc::clone(b.value());
        }
        Arc::clone(
            self.inner
                .entry(run_id.to_string())
                .or_insert_with(|| Arc::new(RunBuffer::new()))
                .value(),
        )
    }

    /// Append a captured line for a run (called by [`RunLogLayer::on_event`]).
    pub fn append(&self, run_id: &str, line: String) {
        self.buffer(run_id).push(line);
    }

    /// Open a `/logs` reader: returns the ring snapshot, a live-tail receiver, and
    /// whether the run has already ended. `None` means no buffer exists for the
    /// run (never logged, or already dropped after the drain window).
    ///
    /// Subscribe-then-snapshot-then-load-`ended` ordering is deliberate: a reader
    /// that misses the `End` broadcast still observes `ended == true`, so the
    /// stream can close without hanging. Backfill/live duplicates are removed by
    /// sequence number in [`log_events`].
    pub fn reader(
        &self,
        run_id: &str,
    ) -> Option<(Vec<LogLine>, broadcast::Receiver<LogMsg>, bool)> {
        let buf = Arc::clone(self.inner.get(run_id)?.value());
        let rx = buf.tx.subscribe();
        let snapshot = buf.snapshot();
        let ended = buf.ended.load(Ordering::SeqCst);
        Some((snapshot, rx, ended))
    }

    /// Mark a run terminal: broadcast `End` so live readers can close.
    pub fn finish(&self, run_id: &str) {
        if let Some(buf) = self.inner.get(run_id) {
            buf.finish();
        }
    }

    /// Drop a run's buffer, freeing its ring (called after the drain window).
    pub fn drop_run(&self, run_id: &str) {
        self.inner.remove(run_id);
    }
}

/// An SSE-bound log event, decoupled from `axum`'s `Event` so the streaming logic
/// (ring replay → live tail, de-dup, lag handling) is unit-testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEvent {
    /// A log line.
    Log(String),
    /// `n` live-tail lines were dropped because the reader fell behind.
    Truncated(u64),
    /// Terminal: the run finished and the stream should close.
    End,
}

/// Build the ordered event stream for a `/logs` request: replay the ring
/// snapshot, then forward the live tail, de-duplicating against the snapshot by
/// sequence number and mapping `broadcast` lag to [`LogEvent::Truncated`].
pub fn log_events(
    snapshot: Vec<LogLine>,
    mut rx: broadcast::Receiver<LogMsg>,
    ended: bool,
) -> impl futures::Stream<Item = LogEvent> {
    use tokio::sync::broadcast::error::RecvError;
    async_stream::stream! {
        let mut last_seq: Option<u64> = None;
        for entry in snapshot {
            last_seq = Some(entry.seq);
            yield LogEvent::Log(entry.line);
        }
        // The run already ended before we subscribed: the ring is the whole story.
        if ended {
            yield LogEvent::End;
            return;
        }
        loop {
            match rx.recv().await {
                Ok(LogMsg::Line(entry)) => {
                    if last_seq.is_none_or(|s| entry.seq > s) {
                        last_seq = Some(entry.seq);
                        yield LogEvent::Log(entry.line);
                    }
                }
                Ok(LogMsg::End) => {
                    yield LogEvent::End;
                    break;
                }
                Err(RecvError::Lagged(n)) => {
                    yield LogEvent::Truncated(n);
                }
                Err(RecvError::Closed) => {
                    yield LogEvent::End;
                    break;
                }
            }
        }
    }
}

// ── tracing layer ───────────────────────────────────────────────────────────

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// Span extension marking a span (and, via scope walking, its descendants) as
/// belonging to a serve run.
#[derive(Clone)]
struct RunIdExt(String);

/// Extracts a `serve_run_id` field value from span attributes.
#[derive(Default)]
struct RunIdVisitor(Option<String>);

impl Visit for RunIdVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "serve_run_id" {
            self.0 = Some(value.to_string());
        }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // `%run_id` records via Display → Debug-of-DisplayValue, i.e. unquoted.
        if field.name() == "serve_run_id" && self.0.is_none() {
            self.0 = Some(format!("{value:?}"));
        }
    }
}

/// Formats an event's fields into a single log line: the `message` field, then
/// any remaining `key=value` fields.
#[derive(Default)]
struct EventLineVisitor {
    message: String,
    fields: String,
}

impl Visit for EventLineVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            let _ = write!(self.fields, " {}={:?}", field.name(), value);
        }
    }
}

impl EventLineVisitor {
    fn finish(self) -> String {
        if self.fields.is_empty() {
            self.message
        } else {
            format!("{}{}", self.message, self.fields)
        }
    }
}

/// Tracing layer that captures events tagged with a `serve_run_id` into the
/// [`LogHub`] for SSE streaming. Added to serve's global subscriber alongside the
/// redacting fmt layer (`observability.rs`).
pub struct RunLogLayer {
    hub: LogHub,
}

impl RunLogLayer {
    pub fn new(hub: LogHub) -> Self {
        Self { hub }
    }
}

impl<S> Layer<S> for RunLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut visitor = RunIdVisitor::default();
        attrs.record(&mut visitor);
        if let Some(run_id) = visitor.0
            && let Some(span) = ctx.span(id)
        {
            span.extensions_mut().insert(RunIdExt(run_id));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let Some(run_id) = ctx.event_scope(event).and_then(|scope| {
            scope
                .from_root()
                .find_map(|span| span.extensions().get::<RunIdExt>().map(|ext| ext.0.clone()))
        }) else {
            return;
        };
        let mut visitor = EventLineVisitor::default();
        event.record(&mut visitor);
        let meta = event.metadata();
        let line = format!("{} {}: {}", meta.level(), meta.target(), visitor.finish());
        let line = crate::secrets::registry::redact(&line).into_owned();
        self.hub.append(&run_id, line);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test]
    fn ring_caps_and_orders_by_seq() {
        let hub = LogHub::new();
        for i in 0..(RING_CAPACITY + 5) {
            hub.append("r", format!("line-{i}"));
        }
        let (snapshot, _rx, _ended) = hub.reader("r").unwrap();
        assert_eq!(snapshot.len(), RING_CAPACITY, "ring must cap at capacity");
        // The five oldest lines were evicted; line-5 is now the front.
        assert_eq!(snapshot.first().unwrap().line, "line-5");
        assert!(
            snapshot.windows(2).all(|w| w[0].seq < w[1].seq),
            "sequence numbers must be strictly increasing"
        );
    }

    #[test]
    fn reader_none_for_unknown_run() {
        let hub = LogHub::new();
        assert!(hub.reader("nope").is_none());
    }

    #[test]
    fn finish_sets_ended_flag() {
        let hub = LogHub::new();
        hub.append("r", "x".into());
        hub.finish("r");
        let (snapshot, _rx, ended) = hub.reader("r").unwrap();
        assert!(ended, "reader must observe ended after finish");
        assert_eq!(snapshot.len(), 1);
    }

    #[test]
    fn drop_run_frees_buffer() {
        let hub = LogHub::new();
        hub.append("r", "x".into());
        assert!(hub.reader("r").is_some());
        hub.drop_run("r");
        assert!(hub.reader("r").is_none());
    }

    #[tokio::test]
    async fn ended_buffer_streams_snapshot_then_end() {
        let hub = LogHub::new();
        hub.append("r", "a".into());
        hub.append("r", "b".into());
        hub.finish("r");
        let (snapshot, rx, ended) = hub.reader("r").unwrap();
        let events: Vec<LogEvent> = log_events(snapshot, rx, ended).collect().await;
        assert_eq!(
            events,
            vec![
                LogEvent::Log("a".into()),
                LogEvent::Log("b".into()),
                LogEvent::End
            ]
        );
    }

    #[tokio::test]
    async fn snapshot_then_live_dedups_by_seq() {
        let (tx, rx) = broadcast::channel(8);
        // seq 0 lands in BOTH the snapshot and the broadcast — must not duplicate.
        let _ = tx.send(LogMsg::Line(LogLine {
            seq: 0,
            line: "a".into(),
        }));
        let _ = tx.send(LogMsg::Line(LogLine {
            seq: 1,
            line: "b".into(),
        }));
        let _ = tx.send(LogMsg::End);
        let snapshot = vec![LogLine {
            seq: 0,
            line: "a".into(),
        }];
        let events: Vec<LogEvent> = log_events(snapshot, rx, false).collect().await;
        assert_eq!(
            events,
            vec![
                LogEvent::Log("a".into()),
                LogEvent::Log("b".into()),
                LogEvent::End
            ]
        );
    }

    #[tokio::test]
    async fn truncated_emitted_on_broadcast_lag() {
        // Fill the channel past capacity before the receiver reads → Lagged.
        let (tx, rx) = broadcast::channel(2);
        for i in 0..5u64 {
            let _ = tx.send(LogMsg::Line(LogLine {
                seq: i,
                line: format!("l{i}"),
            }));
        }
        let _ = tx.send(LogMsg::End);
        let events: Vec<LogEvent> = log_events(vec![], rx, false).collect().await;
        assert!(
            events.iter().any(|e| matches!(e, LogEvent::Truncated(_))),
            "a lagging reader must get a Truncated event: {events:?}"
        );
        assert_eq!(events.last(), Some(&LogEvent::End));
    }

    #[test]
    fn layer_captures_events_in_run_span_only() {
        use tracing_subscriber::layer::SubscriberExt;
        let hub = LogHub::new();
        let subscriber = tracing_subscriber::registry().with(RunLogLayer::new(hub.clone()));
        tracing::subscriber::with_default(subscriber, || {
            // An event outside any serve-run span is ignored.
            tracing::info!("orphan event");
            let span = tracing::info_span!("faucet.serve.run", serve_run_id = "run-xyz");
            let _g = span.enter();
            tracing::info!("hello from the run");
        });
        let (snapshot, _rx, _ended) = hub.reader("run-xyz").expect("buffer for the run exists");
        assert!(
            snapshot
                .iter()
                .any(|l| l.line.contains("hello from the run")),
            "in-span event must be captured: {snapshot:?}"
        );
        assert!(
            snapshot.iter().all(|l| !l.line.contains("orphan")),
            "events outside the run span must not be captured"
        );
        // No buffer is created for events that never had a serve_run_id span.
        assert!(hub.reader("nonexistent").is_none());
    }
}
