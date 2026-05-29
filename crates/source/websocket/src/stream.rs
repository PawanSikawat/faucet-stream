//! WebSocket source stream executor.

use crate::config::{WebsocketAuth, WebsocketSourceConfig, decode_frame, shape_record};
use async_trait::async_trait;
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue, header};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Apply `auth` to the HTTP upgrade `request`.
pub(crate) fn apply_auth(request: &mut Request, auth: &WebsocketAuth) -> Result<(), FaucetError> {
    let headers = request.headers_mut();
    match auth {
        WebsocketAuth::None => {}
        WebsocketAuth::Bearer { token } => {
            let value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|e| FaucetError::Config(format!("websocket bearer header: {e}")))?;
            headers.insert(header::AUTHORIZATION, value);
        }
        WebsocketAuth::Custom { headers: custom } => {
            for (k, v) in custom {
                let name = HeaderName::from_bytes(k.as_bytes())
                    .map_err(|e| FaucetError::Config(format!("websocket header name {k}: {e}")))?;
                let value = HeaderValue::from_str(v)
                    .map_err(|e| FaucetError::Config(format!("websocket header value {k}: {e}")))?;
                headers.insert(name, value);
            }
        }
    }
    Ok(())
}

/// A WebSocket streaming source.
pub struct WebsocketSource {
    config: WebsocketSourceConfig,
}

impl WebsocketSource {
    /// Create a new WebSocket source. Validates the config; the connection is
    /// established lazily inside the stream loop (so reconnect can re-establish
    /// it mid-run).
    pub fn new(config: WebsocketSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        Ok(Self { config })
    }

    /// Connect, apply auth + size limits, and send the subscribe frames.
    async fn connect(&self, url: &str) -> Result<WsStream, FaucetError> {
        let mut request = url
            .into_client_request()
            .map_err(|e| FaucetError::Config(format!("websocket url {url}: {e}")))?;
        apply_auth(&mut request, &self.config.auth)?;

        let ws_config = self.config.max_message_bytes.map(|n| {
            WebSocketConfig::default()
                .max_message_size(Some(n))
                .max_frame_size(Some(n))
        });

        let (mut ws, _resp) = connect_async_with_config(request, ws_config, false)
            .await
            .map_err(|e| FaucetError::Source(format!("websocket connect {url}: {e}")))?;

        for msg in &self.config.subscribe_messages {
            ws.send(Message::Text(msg.clone().into()))
                .await
                .map_err(|e| FaucetError::Source(format!("websocket subscribe: {e}")))?;
        }
        Ok(ws)
    }
}

#[async_trait]
impl Source for WebsocketSource {
    /// Drain the entire run window into memory. This buffers every record the
    /// run produces (bounded only by `max_messages` / `idle_timeout`); prefer
    /// [`Source::stream_pages`] for large or long-running feeds so memory stays
    /// bounded at `batch_size`.
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let mut out = Vec::new();
        let mut pages = self.stream_pages(context, self.config.batch_size);
        while let Some(page) = pages.next().await {
            out.extend(page?.records);
        }
        Ok(out)
    }

    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let resolved_url = faucet_core::util::substitute_context(&self.config.url, context);
        let batch_size = self.config.batch_size;
        let page_chunk = if batch_size == 0 {
            usize::MAX
        } else {
            batch_size
        };
        let max_messages = self.config.max_messages.unwrap_or(usize::MAX);
        let idle_timeout = self.config.idle_timeout;
        let reconnect = self.config.reconnect;
        let backoff = self.config.reconnect_backoff;
        let max_attempts = self.config.max_reconnect_attempts;
        let ping_interval = self.config.ping_interval;
        let format = self.config.message_format;
        let on_parse_error = self.config.on_parse_error;
        let envelope = self.config.envelope;

        Box::pin(async_stream::try_stream! {
            let mut buffer: Vec<Value> = Vec::new();
            let mut total: usize = 0;
            let mut last_message_at = Instant::now();
            let mut reconnect_attempts: usize = 0;

            'outer: loop {
                // Idle cap also bounds connect-failure spins and reconnect gaps.
                if let Some(t) = idle_timeout
                    && Instant::now() >= last_message_at + t
                {
                    tracing::debug!("websocket source: idle_timeout reached, stopping");
                    break 'outer;
                }

                // (Re)connect.
                let ws = match self.connect(&resolved_url).await {
                    Ok(ws) => {
                        reconnect_attempts = 0;
                        ws
                    }
                    Err(e) => {
                        if reconnect
                            && max_attempts.is_none_or(|m| reconnect_attempts < m)
                        {
                            reconnect_attempts += 1;
                            tracing::warn!(error = %e, attempt = reconnect_attempts, "websocket source: connect failed, retrying");
                            tokio::time::sleep(backoff).await;
                            continue 'outer;
                        }
                        Err(e)?;
                        break 'outer; // unreachable; satisfies the type checker
                    }
                };

                let (mut write, mut read) = ws.split();
                // Start one interval out so the first `tick()` does not fire
                // immediately (which would send a Ping before any read on every
                // (re)connect). `tokio::time::interval` ticks at t=0.
                let mut ping_timer = ping_interval.map(|interval| {
                    tokio::time::interval_at(tokio::time::Instant::now() + interval, interval)
                });

                loop {
                    let idle_deadline = idle_timeout.map(|t| last_message_at + t);
                    let poll_budget = match idle_deadline {
                        Some(d) => d.saturating_duration_since(Instant::now()),
                        None => Duration::from_secs(3600),
                    };

                    // Flags collected from the select arms; `?` cannot cross
                    // the select match boundary into the try_stream! body.
                    let mut stop = false;
                    let mut fatal: Option<FaucetError> = None;
                    let mut reconnect_now = false;

                    // Decode a data-frame payload (Text or Binary), shape it,
                    // push it, and update the run-window counters. The only
                    // per-arm difference is `t.as_bytes()` vs `&b`, so both
                    // arms funnel through this single closure.
                    let mut handle_payload = |payload: &[u8]| {
                        match decode_frame(format, on_parse_error, payload) {
                            Ok(Some(v)) => {
                                let now = if envelope { now_unix_ms() } else { 0 };
                                buffer.push(shape_record(v, envelope, &resolved_url, now));
                                last_message_at = Instant::now();
                                reconnect_attempts = 0;
                                total += 1;
                                if total >= max_messages {
                                    stop = true;
                                }
                            }
                            Ok(None) => {}
                            Err(e) => fatal = Some(e),
                        }
                    };

                    tokio::select! {
                        biased;
                        _ = tokio::signal::ctrl_c() => {
                            tracing::info!("websocket source: ctrl_c received, stopping cleanly");
                            stop = true;
                        }
                        _ = async { ping_timer.as_mut().unwrap().tick().await }, if ping_timer.is_some() => {
                            if let Err(e) = write.send(Message::Ping(Vec::new().into())).await {
                                tracing::warn!(error = %e, "websocket source: ping failed, treating as disconnect");
                                reconnect_now = true;
                            }
                        }
                        recv = tokio::time::timeout(poll_budget, read.next()) => {
                            match recv {
                                Ok(Some(Ok(msg))) => {
                                    match msg {
                                        Message::Text(t) => handle_payload(t.as_bytes()),
                                        Message::Binary(b) => handle_payload(&b),
                                        Message::Ping(payload) => {
                                            if let Err(e) = write.send(Message::Pong(payload)).await {
                                                tracing::warn!(error = %e, "websocket source: pong failed");
                                                reconnect_now = true;
                                            }
                                        }
                                        Message::Pong(_) | Message::Frame(_) => {}
                                        Message::Close(frame) => {
                                            let clean = frame
                                                .as_ref()
                                                .map(|f| f.code == CloseCode::Normal)
                                                .unwrap_or(true);
                                            if clean && !reconnect {
                                                tracing::info!("websocket source: server closed (1000), stopping");
                                                stop = true;
                                            } else {
                                                tracing::warn!(?frame, "websocket source: connection closed");
                                                reconnect_now = true;
                                            }
                                        }
                                    }
                                }
                                Ok(Some(Err(e))) => {
                                    tracing::warn!(error = %e, "websocket source: read error");
                                    reconnect_now = true;
                                }
                                Ok(None) => {
                                    tracing::warn!("websocket source: stream ended");
                                    reconnect_now = true;
                                }
                                Err(_elapsed) => {
                                    if let Some(d) = idle_deadline
                                        && Instant::now() >= d
                                    {
                                        tracing::debug!("websocket source: idle_timeout reached, stopping");
                                        stop = true;
                                    }
                                }
                            }
                        }
                    }

                    if let Some(e) = fatal {
                        Err(e)?;
                    }

                    if !buffer.is_empty() && buffer.len() >= page_chunk {
                        let page = std::mem::take(&mut buffer);
                        yield StreamPage { records: page, bookmark: None };
                    }

                    if stop {
                        break 'outer;
                    }

                    if reconnect_now {
                        if reconnect && max_attempts.is_none_or(|m| reconnect_attempts < m) {
                            reconnect_attempts += 1;
                            tracing::warn!(attempt = reconnect_attempts, "websocket source: reconnecting");
                            tokio::time::sleep(backoff).await;
                            continue 'outer;
                        } else if reconnect {
                            Err(FaucetError::Source(format!(
                                "websocket source: exceeded max_reconnect_attempts ({})",
                                max_attempts.unwrap_or(0)
                            )))?;
                        } else {
                            Err(FaucetError::Source(
                                "websocket source: connection closed and reconnect=false".into(),
                            ))?;
                        }
                    }
                }
            }

            if !buffer.is_empty() {
                yield StreamPage { records: buffer, bookmark: None };
            }

            tracing::info!(messages = total, "websocket source: stream complete");
        })
    }

    fn config_schema(&self) -> Value {
        let schema = schemars::schema_for!(WebsocketSourceConfig);
        serde_json::to_value(&schema).unwrap_or(Value::Null)
    }

    fn connector_name(&self) -> &'static str {
        "websocket"
    }
}
