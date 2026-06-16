# faucet-source-grpc

[![Crates.io](https://img.shields.io/crates/v/faucet-source-grpc.svg)](https://crates.io/crates/faucet-source-grpc)
[![Docs.rs](https://docs.rs/faucet-source-grpc/badge.svg)](https://docs.rs/faucet-source-grpc)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-grpc.svg)](https://github.com/PawanSikawat/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-grpc.svg)](https://github.com/PawanSikawat/faucet-stream#license)

A config-driven gRPC source that uses protobuf reflection to call any gRPC service dynamically and return records as JSON.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```bash
cargo add faucet-source-grpc
cargo add tokio --features full
```

Or via the umbrella crate:
```bash
cargo add faucet-stream --features source-grpc
```

## Prerequisites

This source requires a compiled `FileDescriptorSet` file. Generate it from your `.proto` files using `protoc`:

```bash
protoc --descriptor_set_out=descriptor.bin --include_imports \
    -I proto/ proto/my_service.proto
```

The descriptor file contains the full schema of your protobuf messages and services, enabling dynamic encoding and decoding without code generation.

## Quick Start

```rust
use faucet_source_grpc::{GrpcStream, GrpcStreamConfig};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = GrpcStreamConfig::new(
        "http://localhost:50051",
        "users.UserService",
        "ListUsers",
        "proto/descriptor.bin",
    )
    .request(json!({"page_size": 100}))
    .records_path("$.users[*]");

    let stream = GrpcStream::new(config)?;
    let records = stream.fetch_all().await?;

    for record in &records {
        println!("{}", record);
    }
    Ok(())
}
```

## Configuration

### GrpcStreamConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | `String` | *(required)* | gRPC endpoint URL (e.g. `"http://localhost:50051"`) |
| `service_name` | `String` | *(required)* | Fully qualified service name (e.g. `"mypackage.MyService"`) |
| `method_name` | `String` | *(required)* | Method name (e.g. `"ListUsers"`) |
| `descriptor_set_path` | `PathBuf` | *(required)* | Path to the compiled `FileDescriptorSet` file |
| `request` | `Value` | `{}` | Request message as JSON. Fields are mapped to protobuf fields using the descriptor |
| `auth` | `GrpcAuth` | `GrpcAuth::None` | Authentication method |
| `tls` | `Option<bool>` | `None` | Whether to use TLS. When `None`, auto-detected from `https://` in the endpoint URL |
| `records_path` | `Option<String>` | `None` | JSONPath to extract records from the response. If not set, the entire response is returned as a single record |
| `batch_size` | `usize` | `1000` | Records per emitted `StreamPage` for `Source::stream_pages`. `0` is the "no batching" sentinel — the entire result set is emitted in a single page. See [Streaming and batching](#streaming-and-batching) below — for unary RPCs `0` and any positive value behave identically |
| `rpc_kind` | `RpcKind` | `Unary` | RPC kind: `Unary` (one request → one response) or `ServerStreaming` (one request → stream of responses). See [Server-streaming RPCs](#server-streaming-rpcs) below |
| `max_messages` | `Option<usize>` | `None` | Server-streaming only. Cap on the number of streamed messages to consume before terminating. `None` means consume until the server closes the stream |
| `terminate_on_error` | `bool` | `false` | Server-streaming only. If `true`, transient stream errors terminate the run. If `false`, the source reconnects with exponential backoff |
| `reconnect_initial_backoff` | `Duration` (secs) | `1` | Server-streaming only. Initial backoff before the first reconnect attempt; doubles after each failure up to `reconnect_max_backoff`. Must be `> 0` (a zero backoff would busy-spin reconnects and is rejected at construction) |
| `reconnect_max_backoff` | `Duration` (secs) | `30` | Server-streaming only. Upper bound on reconnect backoff |
| `reconnect_max_attempts` | `Option<u32>` | `None` | Server-streaming only. Maximum reconnect attempts before surfacing the error. `None` means unlimited |
| `reconnect_replay_from_start` | `bool` | `true` | Server-streaming only. Whether the server replays the stream from message 0 when the same request is re-issued on reconnect. `true` (default) skips already-emitted messages so each is delivered once; `false` emits every received message (at-least-once). See [Reconnect on transient errors](#reconnect-on-transient-errors) |
| `max_decoding_message_size` | `Option<usize>` | `None` | Maximum size in bytes of a single inbound (decoded) message. `None` keeps tonic's built-in 4 MiB limit. Raise for sources that return large messages |
| `max_encoding_message_size` | `Option<usize>` | `None` | Maximum size in bytes of a single outbound (encoded) request message. `None` keeps tonic's built-in limit |

### Authentication (GrpcAuth)

| Variant | Fields | Description |
|---------|--------|-------------|
| `None` | -- | No authentication |
| `Bearer { token }` | `String` | Bearer token sent as `authorization` metadata |
| `Metadata { entries }` | `Vec<MetadataEntry { key, value }>` | Custom metadata pairs attached to the gRPC request — `Vec` preserves order and allows duplicate keys (gRPC permits both) |

## Streaming and batching

### Unary RPCs

Unary gRPC returns one response containing all records; `stream_pages` falls back to the default trait impl, which buffers the full response and then chunks it in memory into `batch_size` pages. This bounds **sink-side** memory only — source-side memory is still O(full response).

`batch_size = 0` and any positive `batch_size` are observably identical for unary gRPC — both buffer the full result before yielding, since the unary RPC has no native paging primitive the source could honour. Treat the field as a sink-side chunk size, not a wire-protocol hint.

### Server-streaming RPCs

When `rpc_kind = "server_streaming"`, the source calls `tonic::client::Grpc::server_streaming` and consumes the response stream message-by-message. Each streamed `DynamicMessage` is decoded via `prost-reflect` and converted to JSON; if `records_path` is set it is applied to each message individually (so `$.events[*]` flattens an array nested inside each message into the page record set).

`stream_pages` flushes a page each time the buffer accumulates `batch_size` records, bounding both **source-side and sink-side** memory. `batch_size = 0` drains the entire stream into a single page (useful for short streams or sinks that prefer one large write). Pages carry `bookmark: None` — server-streaming has no native cursor, so resumption is driven by user-supplied request fields (e.g. an event id), not a faucet-managed bookmark.

#### Reconnect on transient errors

By default, transient stream errors (server disconnects, transport failures, etc.) trigger a reconnect with exponential backoff starting at `reconnect_initial_backoff`, doubling each attempt up to `reconnect_max_backoff`. After `reconnect_max_attempts` (when set), the error is surfaced. Set `terminate_on_error = true` to skip the reconnect path entirely and propagate the error on first failure.

Reconnect re-sends the *same* request from scratch (the request is resolved once per run), so a stateless server begins emitting from the start of the stream again. By default (`reconnect_replay_from_start = true`) the source tracks how many messages it has already emitted and **skips that replayed prefix** on the reconnected attempt, so each message is delivered downstream exactly once.

Set `reconnect_replay_from_start = false` only for servers that resume mid-stream on an identical request (rare — most resumable feeds require a resume token *in the request*, e.g. an `after_event_id` field the user maintains). With it `false`, every received message is emitted (at-least-once), so a replayed prefix produces duplicates downstream; conversely, leaving it `true` against a genuinely-resuming server would skip legitimate new messages. Match the setting to the server's replay semantics.

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_grpc::GrpcStreamConfig;

let config: GrpcStreamConfig = load_json("config.json")?;
let config: GrpcStreamConfig = load_env_file(".env", "GRPC")?;
```

### Example JSON config (unary)

```json
{
  "endpoint": "https://grpc.example.com:443",
  "service_name": "inventory.InventoryService",
  "method_name": "ListProducts",
  "descriptor_set_path": "proto/descriptor.bin",
  "request": {
    "category": "electronics",
    "page_size": 100
  },
  "auth": {
    "type": "bearer",
    "config": {
      "token": "your-api-token"
    }
  },
  "tls": true,
  "records_path": "$.products[*]",
  "batch_size": 1000
}
```

### Example JSON config (server-streaming)

```json
{
  "endpoint": "https://grpc.example.com:443",
  "service_name": "events.EventService",
  "method_name": "Tail",
  "descriptor_set_path": "proto/descriptor.bin",
  "request": { "topic": "audit-log" },
  "auth": { "type": "bearer", "config": { "token": "your-api-token" } },
  "tls": true,
  "records_path": null,
  "rpc_kind": "server_streaming",
  "max_messages": 100000,
  "terminate_on_error": false,
  "reconnect_initial_backoff": 1,
  "reconnect_max_backoff": 30,
  "reconnect_max_attempts": null,
  "reconnect_replay_from_start": true,
  "max_decoding_message_size": 16777216,
  "batch_size": 500
}
```

### Example .env file

```env
GRPC_ENDPOINT=http://localhost:50051
GRPC_SERVICE_NAME=users.UserService
GRPC_METHOD_NAME=ListUsers
GRPC_DESCRIPTOR_SET_PATH=proto/descriptor.bin
```

## Config Schema Introspection

```rust
use faucet_core::Source;

let stream = GrpcStream::new(config)?;
let schema = stream.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Examples

### Basic unary RPC call

```rust
use faucet_source_grpc::{GrpcStream, GrpcStreamConfig};
use serde_json::json;

let config = GrpcStreamConfig::new(
    "http://localhost:50051",
    "orders.OrderService",
    "GetOrder",
    "proto/descriptor.bin",
)
.request(json!({"order_id": "ord-12345"}));

let stream = GrpcStream::new(config)?;
let records = stream.fetch_all().await?;
// Returns the full response as a single JSON record
```

### Authenticated gRPC with TLS and record extraction

```rust
use faucet_source_grpc::{GrpcStream, GrpcStreamConfig, GrpcAuth};
use serde_json::json;

let config = GrpcStreamConfig::new(
    "https://grpc.production.example.com",
    "analytics.EventService",
    "QueryEvents",
    "proto/descriptor.bin",
)
.request(json!({
    "start_time": "2025-01-01T00:00:00Z",
    "end_time": "2025-02-01T00:00:00Z",
    "limit": 1000
}))
.auth(GrpcAuth::Bearer {
    token: "your-bearer-token".into(),
})
.tls(true)
.records_path("$.events[*]");

let stream = GrpcStream::new(config)?;
let events = stream.fetch_all().await?;
println!("Fetched {} events", events.len());
```

### Server-streaming RPC

```rust
use faucet_source_grpc::{GrpcStream, GrpcStreamConfig, RpcKind};
use serde_json::json;

let config = GrpcStreamConfig::new(
    "http://localhost:50051",
    "events.EventService",
    "Tail",
    "proto/descriptor.bin",
)
.request(json!({ "topic": "audit-log" }))
.rpc_kind(RpcKind::ServerStreaming)
.max_messages(10_000)
.with_batch_size(500);

let stream = GrpcStream::new(config)?;
let events = stream.fetch_all().await?;
println!("Collected {} events", events.len());
```

For long-lived streams, drive the pipeline directly so pages flush to the
sink as they arrive instead of buffering everything in memory:

```rust
use faucet_core::Pipeline;

let pipeline = Pipeline::new(&stream, &my_sink);
pipeline.run().await?;
```

### Custom metadata authentication

```rust
use faucet_source_grpc::{GrpcAuth, GrpcStream, GrpcStreamConfig, MetadataEntry};

let config = GrpcStreamConfig::new(
    "http://localhost:50051",
    "mypackage.MyService",
    "ListItems",
    "proto/descriptor.bin",
)
.auth(GrpcAuth::Metadata {
    entries: vec![
        MetadataEntry { key: "x-api-key".into(), value: "my-secret-key".into() },
        MetadataEntry { key: "x-tenant-id".into(), value: "tenant-123".into() },
    ],
});

let stream = GrpcStream::new(config)?;
let items = stream.fetch_all().await?;
```

## Lineage dataset URI

`<endpoint>/<service_name>/<method_name>` (credentials stripped) — e.g. `http://grpc.example.com:50051/example.Service/ListItems`.

## License

Licensed under MIT or Apache-2.0.
