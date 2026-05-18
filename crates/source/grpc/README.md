# faucet-source-grpc

[![Crates.io](https://img.shields.io/crates/v/faucet-source-grpc.svg)](https://crates.io/crates/faucet-source-grpc)
[![Docs.rs](https://docs.rs/faucet-source-grpc/badge.svg)](https://docs.rs/faucet-source-grpc)

A config-driven gRPC source that uses protobuf reflection to call any gRPC service dynamically and return records as JSON.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```toml
[dependencies]
faucet-source-grpc = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:
```toml
faucet-stream = { version = "0.2", features = ["source-grpc"] }
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

### Authentication (GrpcAuth)

| Variant | Fields | Description |
|---------|--------|-------------|
| `None` | -- | No authentication |
| `Bearer { token }` | `String` | Bearer token sent as `authorization` metadata |
| `Metadata { entries }` | `Vec<MetadataEntry { key, value }>` | Custom metadata pairs attached to the gRPC request — `Vec` preserves order and allows duplicate keys (gRPC permits both) |

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_grpc::GrpcStreamConfig;

let config: GrpcStreamConfig = load_json("config.json")?;
let config: GrpcStreamConfig = load_env_file(".env", "GRPC")?;
```

### Example JSON config

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
    "type": "Bearer",
    "token": "your-api-token"
  },
  "tls": true,
  "records_path": "$.products[*]"
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

## License

Licensed under MIT or Apache-2.0.
