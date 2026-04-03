# faucet-sink-snowflake

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-snowflake.svg)](https://crates.io/crates/faucet-sink-snowflake)
[![Docs.rs](https://docs.rs/faucet-sink-snowflake/badge.svg)](https://docs.rs/faucet-sink-snowflake)

Snowflake sink connector for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

Writes JSON records to a Snowflake table using the Snowflake SQL REST API. Supports JWT key-pair authentication and OAuth. Records are inserted using `PARSE_JSON` with `FLATTEN` for efficient batch loading. All table and schema identifiers are quoted to prevent SQL injection.

## Installation

```toml
[dependencies]
faucet-sink-snowflake = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:

```toml
faucet-stream = { version = "0.2", features = ["sink-snowflake"] }
```

## Quick Start

```rust
use faucet_sink_snowflake::{SnowflakeSink, SnowflakeSinkConfig, SnowflakeAuth};
use faucet_core::Sink;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SnowflakeSinkConfig::new(
        "xy12345.us-east-1",
        "COMPUTE_WH",
        "ANALYTICS_DB",
        "PUBLIC",
        "events",
        SnowflakeAuth::OAuth { token: std::env::var("SNOWFLAKE_TOKEN")? },
    )
    .batch_size(500);

    let sink = SnowflakeSink::new(config);

    let records = vec![
        json!({"user_id": "u123", "event": "page_view", "ts": "2026-04-02T10:00:00Z"}),
        json!({"user_id": "u456", "event": "click", "ts": "2026-04-02T10:01:00Z"}),
    ];

    let rows_written = sink.write_batch(&records).await?;
    println!("Wrote {rows_written} rows to Snowflake");

    Ok(())
}
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `account` | `String` | *(required)* | Snowflake account identifier (e.g. `"xy12345.us-east-1"`) |
| `warehouse` | `String` | *(required)* | Warehouse to use for the session |
| `database` | `String` | *(required)* | Database name |
| `schema` | `String` | *(required)* | Schema name |
| `table` | `String` | *(required)* | Target table name |
| `auth` | `SnowflakeAuth` | *(required)* | Authentication credentials (see below) |
| `batch_size` | `usize` | `500` | Maximum number of rows per INSERT statement |

### Authentication (`SnowflakeAuth`)

| Variant | Fields | Description |
|---------|--------|-------------|
| `KeyPair` | `user: String`, `private_key_pem: String` | JWT key-pair authentication using an RSA private key (PEM-encoded). Generates RS256 JWT tokens with the Snowflake `ACCOUNT.USER` as the issuer and subject. Tokens are valid for 1 hour. |
| `OAuth` | `token: String` | OAuth2 bearer token from an external identity provider. Sent as `Snowflake Token="..."` in the Authorization header. |

The `Debug` implementation masks `private_key_pem` and `token` with `***` to prevent credential leakage in logs.

### Builder Methods

```rust
use faucet_sink_snowflake::{SnowflakeSinkConfig, SnowflakeAuth};

let config = SnowflakeSinkConfig::new(
    "xy12345.us-east-1",
    "COMPUTE_WH",
    "MY_DB",
    "PUBLIC",
    "events",
    SnowflakeAuth::OAuth { token: "my-oauth-token".into() },
)
.batch_size(1000);
```

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_snowflake::SnowflakeSinkConfig;

// From a JSON file
let config: SnowflakeSinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: SnowflakeSinkConfig = load_env_file(".env", "SNOWFLAKE")?;
```

### Example JSON config (OAuth)

```json
{
  "account": "xy12345.us-east-1",
  "warehouse": "COMPUTE_WH",
  "database": "ANALYTICS_DB",
  "schema": "PUBLIC",
  "table": "events",
  "auth": {
    "type": "OAuth",
    "token": "eyJhbGciOiJSUzI1NiIs..."
  },
  "batch_size": 500
}
```

### Example JSON config (KeyPair JWT)

```json
{
  "account": "xy12345.us-east-1",
  "warehouse": "COMPUTE_WH",
  "database": "ANALYTICS_DB",
  "schema": "RAW",
  "table": "ingest_events",
  "auth": {
    "type": "KeyPair",
    "user": "DATA_LOADER",
    "private_key_pem": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBAD..."
  },
  "batch_size": 250
}
```

### Example .env file

```env
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ANALYTICS_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_TABLE=events
SNOWFLAKE_AUTH='{"type":"OAuth","token":"eyJhbGciOiJSUzI1NiIs..."}'
SNOWFLAKE_BATCH_SIZE=500
```

## Config Schema Introspection

```rust
use faucet_core::Sink;

let sink = SnowflakeSink::new(config);
let schema = sink.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Pipeline Usage

```rust
use faucet_core::Pipeline;
use faucet_source_rest::{RestStream, RestStreamConfig};
use faucet_sink_snowflake::{SnowflakeSink, SnowflakeSinkConfig, SnowflakeAuth};

let source = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/v1/events")
);

let sink = SnowflakeSink::new(SnowflakeSinkConfig::new(
    "xy12345.us-east-1",
    "COMPUTE_WH",
    "ANALYTICS_DB",
    "PUBLIC",
    "events",
    SnowflakeAuth::OAuth { token: "my-token".into() },
));

let result = Pipeline::new(source, sink).run().await?;
println!("Transferred {} records", result.records_written);
```

## Examples

### JWT key-pair authentication

```rust
let private_key = std::fs::read_to_string("/path/to/rsa_key.pem")?;

let config = SnowflakeSinkConfig::new(
    "xy12345.us-east-1",
    "ETL_WH",
    "RAW_DB",
    "INGEST",
    "api_events",
    SnowflakeAuth::KeyPair {
        user: "ETL_SERVICE_USER".into(),
        private_key_pem: private_key,
    },
)
.batch_size(500);

let sink = SnowflakeSink::new(config);
sink.write_batch(&records).await?;
```

### OAuth authentication

```rust
let token = std::env::var("SNOWFLAKE_OAUTH_TOKEN")?;

let config = SnowflakeSinkConfig::new(
    "xy12345.us-east-1",
    "COMPUTE_WH",
    "ANALYTICS",
    "PUBLIC",
    "metrics",
    SnowflakeAuth::OAuth { token },
);

let sink = SnowflakeSink::new(config);
sink.write_batch(&records).await?;
```

### Small batch sizes for latency-sensitive workloads

```rust
let config = SnowflakeSinkConfig::new(
    "xy12345.us-east-1",
    "COMPUTE_WH",
    "MY_DB",
    "PUBLIC",
    "realtime_events",
    SnowflakeAuth::OAuth { token: "...".into() },
)
.batch_size(50);

let sink = SnowflakeSink::new(config);
```

## How It Works

- `SnowflakeSink::new()` creates an HTTP client (reused across all requests) but does not make any network calls.
- `write_batch()` splits records into chunks of `batch_size`. For each chunk, it builds an INSERT statement using `PARSE_JSON` with `FLATTEN` to parse a JSON array and insert all rows in a single SQL statement.
- The SQL statement targets the fully qualified table name `"database"."schema"."table"` with quoted identifiers.
- Authentication headers are generated per request: JWT tokens for KeyPair auth (with 1-hour expiry), or the `Snowflake Token="..."` header for OAuth.
- The Snowflake SQL REST API endpoint is `https://{account}.snowflakecomputing.com/api/v2/statements`.
- Successful execution is validated by checking for the `090001` response code ("Statement executed successfully").

## License

Licensed under MIT or Apache-2.0.
