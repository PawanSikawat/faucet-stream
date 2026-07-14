//! Live terminal UI for `faucet run --tui` (#203, `cli-tui` feature).
//!
//! The pipeline is untouched: it emits the same `metrics` series it always
//! does, and the TUI samples the in-process Prometheus recorder's rendered
//! text a few times per second (read-only — zero hot-path impact) and draws
//! a full-screen [ratatui] view: per-invocation throughput, errors, DLQ
//! counts, bookmark age, and a live log pane. `q` (or `Ctrl-C`, which raw
//! mode delivers as a key event) cancels cooperatively via the executor's
//! [`CancellationToken`] — in-flight invocations stop at their next page
//! boundary and flush their sinks.
//!
//! Log handling: the normal stdout subscriber would corrupt the alternate
//! screen, so when a TUI session is detected at startup (`--tui` on a real
//! TTY), `run_main` routes the fmt subscriber into an in-memory ring
//! ([`log_buffer`]) that the TUI renders as its log pane. Lines are redacted
//! at capture with the same registry the stdout writer uses. On a non-TTY
//! (CI, pipes) the flag degrades to a plain run with a one-line notice.

pub mod metrics;
pub mod view;

use crate::error::{CliError, CliResult};
use faucet_core::CancellationToken;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::collections::VecDeque;
use std::io::IsTerminal;
use std::sync::{Arc, Mutex, OnceLock};

/// Ring-buffered log lines shared between the tracing subscriber (writer
/// side, installed in `run_main`) and the TUI (render side).
#[derive(Clone, Default)]
pub struct LogBuffer {
    inner: Arc<Mutex<VecDeque<String>>>,
}

const LOG_BUFFER_CAP: usize = 200;

impl LogBuffer {
    /// Append one already-formatted subscriber line (redacted at capture).
    pub fn push_line(&self, line: &str) {
        let line = crate::secrets::registry::redact(line).into_owned();
        let mut q = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if q.len() == LOG_BUFFER_CAP {
            q.pop_front();
        }
        q.push_back(line);
    }

    /// Snapshot the buffered lines (oldest first).
    pub fn snapshot(&self) -> Vec<String> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .cloned()
            .collect()
    }
}

/// Line-splitting `io::Write` adapter for the tracing subscriber.
pub struct LogBufferWriter {
    buffer: LogBuffer,
    partial: String,
}

impl std::io::Write for LogBufferWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.partial.push_str(&String::from_utf8_lossy(buf));
        while let Some(at) = self.partial.find('\n') {
            let line: String = self.partial.drain(..=at).collect();
            let line = line.trim_end();
            if !line.is_empty() {
                self.buffer.push_line(line);
            }
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
    type Writer = LogBufferWriter;
    fn make_writer(&'a self) -> Self::Writer {
        LogBufferWriter {
            buffer: self.clone(),
            partial: String::new(),
        }
    }
}

/// The process-wide TUI log ring. Populated by `run_main` when it detects a
/// TUI session before installing the subscriber; read by the render loop.
static TUI_LOGS: OnceLock<LogBuffer> = OnceLock::new();

/// `--tui` was passed *and* stdout is a real terminal — the full-screen UI
/// applies. On a non-TTY the caller degrades to a plain run.
pub fn is_tui_session(tui_flag: bool) -> bool {
    tui_flag && std::io::stdout().is_terminal()
}

/// Install the ring-buffered tracing subscriber for a TUI session. Called by
/// `run_main` *instead of* the stdout subscriber, before any log line is
/// emitted. Returns the buffer for the render loop.
pub fn install_tui_tracing(level: &str) -> LogBuffer {
    use tracing_subscriber::EnvFilter;
    let buffer = TUI_LOGS.get_or_init(LogBuffer::default).clone();
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_writer(buffer.clone())
        .try_init();
    buffer
}

/// The TUI log ring, if `run_main` installed one this process.
pub fn log_buffer() -> Option<LogBuffer> {
    TUI_LOGS.get().cloned()
}

/// Default histogram buckets — mirrors `faucet-core`'s installer so a
/// TUI-owned recorder behaves identically for any `/metrics` scraper.
const DEFAULT_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0,
];

/// Install the Prometheus recorder for a TUI session and return its render
/// handle. When the config carries an `observability.prometheus` block the
/// `/metrics` HTTP endpoint is preserved (recorder + listener, exactly what
/// `install_observability` would have set up); otherwise a listener-less
/// recorder is installed purely as the TUI's data source.
pub fn install_metrics_recorder(
    prom: Option<&faucet_core::PrometheusConfig>,
) -> CliResult<PrometheusHandle> {
    let already = |handle: PrometheusHandle, ok: bool| {
        if !ok {
            tracing::warn!(
                "metrics recorder already installed; the TUI may show no data (was a recorder installed before `faucet run --tui`?)"
            );
        }
        Ok(handle)
    };
    match prom {
        None => {
            let recorder = PrometheusBuilder::new()
                .set_buckets(DEFAULT_BUCKETS)
                .map_err(|e| CliError::Observability(e.to_string()))?
                .build_recorder();
            let handle = recorder.handle();
            let ok = ::metrics::set_global_recorder(recorder).is_ok();
            already(handle, ok)
        }
        Some(p) => {
            let listen: std::net::SocketAddr = p
                .listen
                .parse()
                .map_err(|e| CliError::Observability(format!("prometheus listen: {e}")))?;
            let (recorder, exporter) = PrometheusBuilder::new()
                .with_http_listener(listen)
                .set_buckets(p.buckets.as_deref().unwrap_or(DEFAULT_BUCKETS))
                .map_err(|e| CliError::Observability(e.to_string()))?
                .build()
                .map_err(|e| CliError::Observability(e.to_string()))?;
            let handle = recorder.handle();
            let ok = ::metrics::set_global_recorder(recorder).is_ok();
            if ok {
                tokio::spawn(exporter);
                tracing::info!("Prometheus /metrics listening on {}", p.listen);
            }
            already(handle, ok)
        }
    }
}

/// Drive the run future under the full-screen TUI. Returns the run's result
/// after the terminal is restored. `cancel` is the token wired into
/// `ExecuteOptions.cancel`; `q` / `Ctrl-C` trigger it.
pub async fn drive<T>(
    run: impl Future<Output = T>,
    pipeline: &str,
    handle: PrometheusHandle,
    cancel: CancellationToken,
) -> T {
    use ratatui::crossterm::event::{Event, KeyCode, KeyModifiers};

    // `ratatui::init` enters the alternate screen + raw mode and installs a
    // panic hook that restores the terminal before the default hook runs.
    let mut terminal = ratatui::init();
    let started = std::time::Instant::now();
    let mut sampler = metrics::Sampler::new(pipeline);
    let logs = log_buffer().unwrap_or_default();
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    tokio::pin!(run);
    let result = loop {
        tokio::select! {
            biased;
            result = &mut run => break result,
            _ = interval.tick() => {
                // Non-blocking key poll: q / Ctrl-C cancel cooperatively.
                while ratatui::crossterm::event::poll(std::time::Duration::ZERO).unwrap_or(false) {
                    match ratatui::crossterm::event::read() {
                        Ok(Event::Key(key)) => {
                            let ctrl_c = key.code == KeyCode::Char('c')
                                && key.modifiers.contains(KeyModifiers::CONTROL);
                            if key.code == KeyCode::Char('q') || ctrl_c {
                                cancel.cancel();
                            }
                        }
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
                handle.run_upkeep();
                let model = sampler.observe(&handle.render(), started.elapsed());
                let log_lines = logs.snapshot();
                let cancelling = cancel.is_cancelled();
                let _ = terminal.draw(|frame| {
                    view::draw(frame, pipeline, &model, started.elapsed(), &log_lines, cancelling);
                });
            }
        }
    };
    ratatui::restore();
    result
}

/// Flush the tail of the buffered log ring to stderr — called after terminal
/// restore when the run failed, so the operator keeps the context that was
/// on screen.
pub fn flush_logs_to_stderr(max_lines: usize) {
    if let Some(buffer) = log_buffer() {
        let lines = buffer.snapshot();
        let start = lines.len().saturating_sub(max_lines);
        for line in &lines[start..] {
            eprintln!("{line}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn log_buffer_caps_and_orders_lines() {
        let buffer = LogBuffer::default();
        for i in 0..(LOG_BUFFER_CAP + 10) {
            buffer.push_line(&format!("line {i}"));
        }
        let snap = buffer.snapshot();
        assert_eq!(snap.len(), LOG_BUFFER_CAP);
        assert_eq!(snap.first().unwrap(), "line 10");
        assert_eq!(
            snap.last().unwrap(),
            &format!("line {}", LOG_BUFFER_CAP + 9)
        );
    }

    #[test]
    fn writer_splits_lines_and_keeps_partials() {
        let buffer = LogBuffer::default();
        let mut writer = LogBufferWriter {
            buffer: buffer.clone(),
            partial: String::new(),
        };
        writer.write_all(b"first line\nsecond ").unwrap();
        writer.write_all(b"half\ntrailing").unwrap();
        let snap = buffer.snapshot();
        assert_eq!(
            snap,
            vec!["first line".to_string(), "second half".to_string()]
        );
    }

    #[test]
    fn log_lines_are_redacted_at_capture() {
        crate::secrets::registry::register("tui-secret-value");
        let buffer = LogBuffer::default();
        buffer.push_line("token is tui-secret-value here");
        let snap = buffer.snapshot();
        assert!(!snap[0].contains("tui-secret-value"), "got: {}", snap[0]);
    }

    #[test]
    fn non_tty_is_not_a_tui_session() {
        // Test harnesses never run on a TTY stdout, so the flag alone must
        // not start a session — this is the CI/pipe fallback contract.
        assert!(!is_tui_session(true) || std::io::stdout().is_terminal());
        assert!(!is_tui_session(false));
    }
}
