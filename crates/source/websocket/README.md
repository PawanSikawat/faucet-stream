# faucet-source-websocket

[![Crates.io](https://img.shields.io/crates/v/faucet-source-websocket.svg)](https://crates.io/crates/faucet-source-websocket)
[![Docs.rs](https://docs.rs/faucet-source-websocket/badge.svg)](https://docs.rs/faucet-source-websocket)

A WebSocket streaming source: connects to a `ws://`/`wss://` endpoint,
optionally sends subscription frames, and streams each incoming message as a
record until `max_messages`, `idle_timeout`, or Ctrl-C ends the run.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```bash
cargo add faucet-source-websocket
cargo add tokio --features full
```

Or via the umbrella crate:
```bash
cargo add faucet-stream --features source-websocket
```

## Quick Start — Binance trade stream

```rust
use faucet_source_websocket::{WebsocketSource, WebsocketSourceConfig, WsMessageFormat};
use faucet_core::Source;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = WebsocketSourceConfig {
        url: "wss://stream.binance.com:9443/ws/btcusdt@trade".into(),
        message_format: WsMessageFormat::Json,
        max_messages: Some(100),
        idle_timeout: Some(std::time::Duration::from_secs(30)),
        ..serde_json::from_value(serde_json::json!({
            "url": "wss://stream.binance.com:9443/ws/btcusdt@trade"
        }))?
    };
    let source = WebsocketSource::new(config)?;
    let records = source.fetch_all().await?;
    println!("got {} trades", records.len());
    Ok(())
}
```

## Config

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `url` | string | — | `ws://` or `wss://`; supports `{ctx}` substitution |
| `auth` | enum | `none` | `none` / `bearer{token}` / `custom{headers}` |
| `subscribe_messages` | string[] | `[]` | Sent in order on every (re)connect |
| `message_format` | enum | `json` | `json` / `raw_string` / `binary` (base64) |
| `on_parse_error` | enum | `fail` | `fail` / `skip` (json mode) |
| `envelope` | bool | `false` | `true` → `{data,received_at,url}` |
| `ping_interval` | secs | — | Client keepalive ping |
| `max_messages` | int | — | At least one of `max_messages`/`idle_timeout` required |
| `idle_timeout` | secs | — | Stop after this long with no frame received. Reset by **any** inbound frame — data, ping/pong, or a frame dropped by `on_parse_error=skip` — so a live server streaming skippable frames does not trip it. Also caps outages |
| `reconnect` | bool | `false` | Reconnect on drop / non-1000 close |
| `reconnect_backoff` | secs | `1` | Fixed wait between attempts |
| `max_reconnect_attempts` | int | — | Cap consecutive failures (`None` = unlimited) |
| `max_message_bytes` | int | — | Bound frame/message size |
| `batch_size` | int | `1000` | Records per `StreamPage`; `0` = one page |

## Behavior

- **Streaming:** `stream_pages` yields a page each `batch_size` records; the
  sink receives data continuously throughout the run.
- **Not resumable:** a live feed has no replayable offset, so there is no
  state/bookmark — on reconnect the source resubscribes to the live stream.
- **Termination:** `max_messages`, `idle_timeout`, Ctrl-C, or a clean `1000`
  close with `reconnect=false`.

## Lineage dataset URI

`wss://<host><path>` or `ws://<host><path>` (credentials stripped) — e.g. `wss://stream.example.com/feed`.

## License

MIT OR Apache-2.0
