//! Integration tests against an in-process tokio-tungstenite server.

use faucet_core::Source;
use faucet_source_websocket::{
    OnParseError, WebsocketAuth, WebsocketSource, WebsocketSourceConfig, WsMessageFormat,
};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

fn base_config(url: &str) -> WebsocketSourceConfig {
    WebsocketSourceConfig {
        url: url.to_string(),
        auth: WebsocketAuth::None,
        subscribe_messages: vec![],
        message_format: WsMessageFormat::Json,
        on_parse_error: OnParseError::Fail,
        envelope: false,
        ping_interval: None,
        max_messages: None,
        idle_timeout: None,
        reconnect: false,
        reconnect_backoff: Duration::from_millis(50),
        max_reconnect_attempts: None,
        max_message_bytes: None,
        batch_size: 1000,
    }
}

/// Spawn a server that pushes `messages` then holds the connection open.
async fn spawn_pushing_server(messages: Vec<String>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            for m in messages {
                if ws.send(Message::Text(m.into())).await.is_err() {
                    return;
                }
            }
            // Keep the connection open so the client terminates via its own
            // limit, not a server close.
            loop {
                if ws.next().await.is_none() {
                    break;
                }
            }
        }
    });
    format!("ws://{addr}")
}

#[tokio::test]
async fn collects_up_to_max_messages() {
    let url = spawn_pushing_server(vec![
        r#"{"id":1}"#.into(),
        r#"{"id":2}"#.into(),
        r#"{"id":3}"#.into(),
    ])
    .await;
    let mut cfg = base_config(&url);
    cfg.max_messages = Some(3);
    cfg.idle_timeout = Some(Duration::from_secs(5));
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["id"], 1);
    assert_eq!(records[2]["id"], 3);
}

#[tokio::test]
async fn idle_timeout_terminates_quiet_stream() {
    // Server pushes 2 then goes silent; idle_timeout ends the run.
    let url = spawn_pushing_server(vec![r#"{"id":1}"#.into(), r#"{"id":2}"#.into()]).await;
    let mut cfg = base_config(&url);
    cfg.idle_timeout = Some(Duration::from_millis(300));
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
}

#[tokio::test]
async fn binary_frames_base64_encoded() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = ws.send(Message::Binary(b"hello".to_vec().into())).await;
            loop {
                if ws.next().await.is_none() {
                    break;
                }
            }
        }
    });
    let mut cfg = base_config(&format!("ws://{addr}"));
    cfg.message_format = WsMessageFormat::Binary;
    cfg.max_messages = Some(1);
    cfg.idle_timeout = Some(Duration::from_secs(5));
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0], serde_json::json!("aGVsbG8="));
}

#[tokio::test]
async fn envelope_mode_wraps_record() {
    let url = spawn_pushing_server(vec![r#"{"id":1}"#.into()]).await;
    let mut cfg = base_config(&url);
    cfg.envelope = true;
    cfg.max_messages = Some(1);
    cfg.idle_timeout = Some(Duration::from_secs(5));
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records[0]["data"], serde_json::json!({"id": 1}));
    assert!(records[0]["received_at"].is_number());
    assert_eq!(records[0]["url"], url);
}

#[tokio::test]
async fn skip_drops_malformed_json() {
    let url = spawn_pushing_server(vec!["not json".into(), r#"{"id":2}"#.into()]).await;
    let mut cfg = base_config(&url);
    cfg.on_parse_error = OnParseError::Skip;
    cfg.max_messages = Some(1); // the one valid record
    cfg.idle_timeout = Some(Duration::from_secs(5));
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], 2);
}

#[tokio::test]
async fn auth_header_is_sent() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let seen2 = Arc::clone(&seen);
    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            // The `accept_hdr_async` callback's return type is fixed by the
            // tungstenite `Callback` trait (`Response<Option<String>>` is large
            // by value); we cannot box it here, so allow the lint test-side.
            #[allow(clippy::result_large_err)]
            let callback = |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                            resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                if req
                    .headers()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    == Some("Bearer secret")
                {
                    seen2.store(1, Ordering::SeqCst);
                }
                Ok(resp)
            };
            let mut ws = tokio_tungstenite::accept_hdr_async(stream, callback)
                .await
                .unwrap();
            let _ = ws.send(Message::Text(r#"{"id":1}"#.into())).await;
            loop {
                if ws.next().await.is_none() {
                    break;
                }
            }
        }
    });
    let mut cfg = base_config(&format!("ws://{addr}"));
    cfg.auth = WebsocketAuth::Bearer {
        token: "secret".into(),
    };
    cfg.max_messages = Some(1);
    cfg.idle_timeout = Some(Duration::from_secs(5));
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "server did not see the Authorization header"
    );
}

#[tokio::test]
async fn reconnect_resumes_after_drop() {
    // Server closes after the first message on the first connection, then
    // serves a second message on the next connection. Count connections.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut conn = 0u32;
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            conn += 1;
            let n = conn;
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                // Each connection sends one message tagged with the conn number,
                // then closes (drops) to force the client to reconnect.
                let _ = ws
                    .send(Message::Text(format!(r#"{{"conn":{n}}}"#).into()))
                    .await;
                let _ = ws.close(None).await;
            });
        }
    });
    let mut cfg = base_config(&format!("ws://{addr}"));
    cfg.reconnect = true;
    cfg.reconnect_backoff = Duration::from_millis(20);
    cfg.max_messages = Some(2);
    cfg.idle_timeout = Some(Duration::from_secs(5));
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["conn"], 1);
    assert_eq!(records[1]["conn"], 2);
}
