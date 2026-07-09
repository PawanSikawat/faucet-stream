//! [`SingerSource`] — the `Source` implementation that bridges a Singer tap.
//!
//! This is the only place that drives the tap subprocess. Page assembly rules
//! (v0, single-stream):
//!
//! - RECORD for the configured stream → buffered.
//! - STATE → (if `flush_on_state`) flush the buffer as a page whose `bookmark`
//!   is the STATE `value`; otherwise remember it as the pending checkpoint.
//! - buffer reaches the `batch_size` hint → flush a page with `bookmark: None`
//!   (no STATE covers these rows yet; they re-emit from the last checkpoint on
//!   resume and an idempotent sink dedups them).
//! - clean EOF → flush any trailing buffer + pending checkpoint.
//! - non-zero exit / idle timeout / malformed-fail → terminate and error
//!   **without** committing the trailing un-checkpointed buffer.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use faucet_core::{FaucetError, Source, StreamPage, Value, async_trait, schema_for};
use futures::StreamExt;
use futures_core::Stream;

use crate::assemble::PageAssembler;
use crate::config::{MalformedPolicy, SingerSourceConfig};
use crate::message::SingerMessage;
use crate::process::{Line, Recv, TapProcess};

/// A source that runs a Singer tap and adapts its output into faucet records.
///
/// **Tier-2 / experimental.** This reintroduces a runtime (usually Python)
/// dependency for pipelines that use it, throughput is Singer-class rather than
/// faucet-class, and STATE-based resume granularity depends on the individual
/// tap. See the crate README.
pub struct SingerSource {
    config: SingerSourceConfig,
    /// Resume bookmark (the last persisted STATE `value`), injected by the
    /// pipeline via [`apply_start_bookmark`](Source::apply_start_bookmark).
    start_bookmark: Mutex<Option<Value>>,
}

impl SingerSource {
    /// Build a source from its configuration.
    pub fn new(config: SingerSourceConfig) -> Self {
        Self {
            config,
            start_bookmark: Mutex::new(None),
        }
    }
}

#[async_trait]
impl Source for SingerSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        tracing::warn!(
            "SingerSource::fetch_with_context buffers the entire stream in memory, \
             defeating bounded-memory streaming; prefer driving stream_pages via the pipeline"
        );
        // batch_size 0 = single logical batch; we concatenate every page.
        let mut stream = self.stream_pages(context, 0);
        let mut all = Vec::new();
        while let Some(page) = stream.next().await {
            all.extend(page?.records);
        }
        Ok(all)
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        // Take the resume bookmark once at stream construction.
        let start = self.start_bookmark.lock().unwrap().clone();
        let idle = self.config.idle_timeout_secs.map(Duration::from_secs);
        let target_stream = self.config.stream.clone();
        let flush_on_state = self.config.flush_on_state;
        let on_malformed = self.config.on_malformed;

        Box::pin(async_stream::try_stream! {
            let mut process = TapProcess::spawn(&self.config, start.as_ref())?;
            let mut assembler = PageAssembler::new(target_stream, batch_size, flush_on_state);

            loop {
                match process.recv(idle).await {
                    Recv::Line(Line::Parsed(msg)) => match msg {
                        SingerMessage::Record { stream, record } => {
                            if let Some(page) = assembler.on_record(&stream, record) {
                                yield page;
                            }
                        }
                        SingerMessage::State { value } => {
                            if let Some(page) = assembler.on_state(value) {
                                yield page;
                            }
                        }
                        SingerMessage::Schema { stream, .. } => {
                            // v0 pass-through: faucet's sinks infer schema from
                            // records, so the SCHEMA declaration is logged and
                            // skipped.
                            // FUTURE (core RFC): a Source::record_schema() hook
                            // would let us forward this to schema-aware sinks.
                            tracing::debug!(stream = %stream, "singer SCHEMA (v0: pass-through)");
                        }
                        SingerMessage::Other { message_type } => {
                            tracing::debug!(message_type = %message_type, "singer message ignored (v0)");
                        }
                    },
                    Recv::Line(Line::Malformed(reason)) => match on_malformed {
                        MalformedPolicy::Skip => {
                            tracing::warn!(reason = %reason, "skipping malformed tap output line");
                        }
                        MalformedPolicy::Fail => {
                            process.shutdown().await;
                            Err(FaucetError::Source(format!(
                                "malformed tap output line: {reason}"
                            )))?;
                        }
                    },
                    Recv::IdleTimeout => {
                        let tail = process.stderr_tail();
                        process.shutdown().await;
                        Err(FaucetError::Source(format!(
                            "tap produced no output within idle timeout; last stderr:\n{tail}"
                        )))?;
                    }
                    Recv::Eof => {
                        // Determine exit status BEFORE committing the trailing
                        // buffer, so a failed run never writes an
                        // un-checkpointed final page.
                        process.shutdown_and_check().await?;
                        if let Some(page) = assembler.on_eof() {
                            yield page;
                        }
                        break;
                    }
                }
            }
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(schema_for!(SingerSourceConfig)).unwrap_or(Value::Null)
    }

    fn connector_name(&self) -> &'static str {
        "singer"
    }

    fn state_key(&self) -> Option<String> {
        Some(self.config.effective_state_key())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        *self.start_bookmark.lock().unwrap() = Some(bookmark);
        Ok(())
    }
}
