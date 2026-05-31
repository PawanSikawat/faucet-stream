//! `GET /v1/runs/{id}/logs` — Server-Sent Events stream of a run's captured log
//! lines. Replays the run's ring buffer, then forwards the live tail until the
//! run reaches a terminal state. Thin glue over [`crate::serve::logs`].

use crate::serve::error::ServeError;
use crate::serve::logs::{LogEvent, log_events};
use crate::serve::state::ServerState;
use axum::extract::{Path, State};
use axum::response::Sse;
use axum::response::sse::{Event, KeepAlive};
use futures::{Stream, StreamExt};
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::broadcast;

/// Keep-alive comment interval — defeats idle-timeout proxies on a quiet stream.
const KEEP_ALIVE_SECS: u64 = 15;

/// `GET /v1/runs/{id}/logs` → `text/event-stream`.
///
/// Events: `event: log` (a line), `event: truncated` (the reader fell behind and
/// lines were dropped — rely on the centralized log sink), `event: end` (the run
/// finished; the stream closes). 404 if the run is entirely unknown; a known run
/// whose buffer has expired yields a single `end`.
pub async fn stream_logs(
    State(state): State<ServerState>,
    Path(id): Path<String>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ServeError> {
    let (snapshot, rx, ended) = match state.log_hub().reader(&id) {
        Some(reader) => reader,
        None => {
            // No buffer: 404 for an unknown run, or an immediate `end` for a known
            // run whose logs have already been dropped after the drain window.
            let known = state
                .history()
                .get(&id)
                .await
                .map_err(|e| ServeError::Internal(e.to_string()))?
                .is_some();
            if !known {
                return Err(ServeError::NotFound);
            }
            // A dropped sender's receiver is never polled (ended == true).
            (Vec::new(), broadcast::channel(1).1, true)
        }
    };

    let stream =
        log_events(snapshot, rx, ended).map(|ev| Ok::<Event, Infallible>(to_sse_event(ev)));
    Ok(
        Sse::new(stream)
            .keep_alive(KeepAlive::new().interval(Duration::from_secs(KEEP_ALIVE_SECS))),
    )
}

/// Map an internal [`LogEvent`] to an SSE wire event.
fn to_sse_event(ev: LogEvent) -> Event {
    match ev {
        LogEvent::Log(line) => Event::default().event("log").data(line),
        LogEvent::Truncated(n) => Event::default().event("truncated").data(format!(
            "{n} log line(s) dropped; rely on the centralized log sink"
        )),
        LogEvent::End => Event::default().event("end").data("done"),
    }
}
