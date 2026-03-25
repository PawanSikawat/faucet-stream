# faucet-stream

[![Crates.io](https://img.shields.io/crates/v/faucet-stream.svg)](https://crates.io/crates/faucet-stream)
[![Docs.rs](https://docs.rs/faucet-stream/badge.svg)](https://docs.rs/faucet-stream)
[![CI](https://github.com/PawanSikawat/faucet-stream/actions/workflows/ci.yml/badge.svg)](https://github.com/PawanSikawat/faucet-stream/actions)
[![License](https://img.shields.io/crates/l/faucet-stream.svg)](LICENSE-MIT)

A declarative, config-driven REST API client for Rust with pluggable
authentication, pagination, record transforms, schema inference, and
incremental replication.

Inspired by [Meltano's RESTStream](https://sdk.meltano.com/en/latest/classes/singer_sdk.RESTStream.html)
— but for Rust, and as a reusable library.

## Features

- **Authentication** — Bearer, Basic, API Key (header or query param), OAuth2 (client credentials), or custom headers
- **Pagination** — cursor/token (JSONPath), page number, offset/limit, Link header, next-link-in-body
- **JSONPath extraction** — point at where records live in any JSON response
- **Record transforms** — flatten nested objects, rename keys (regex), snake_case normalisation, or custom closures
- **Schema inference** — automatically derive a JSON Schema from sampled records
- **Incremental replication** — bookmark-based filtering so you only fetch new records
- **Partitions** — run the same stream across multiple contexts (e.g. per-org, per-repo)
- **Retries with backoff** — exponential backoff with configurable limits and 429 rate-limit handling
- **Typed deserialization** — get `Vec<Value>` or deserialize directly into your structs
- **Async-first** — built on `reqwest` + `tokio`

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
faucet-stream = "0.1"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

### Cursor-based pagination with Bearer auth

```rust
use faucet_stream::{RestStream, RestStreamConfig, Auth, PaginationStyle};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stream = RestStream::new(
        RestStreamConfig::new("https://api.example.com", "/v1/users")
            .auth(Auth::Bearer("my-token".into()))
            .records_path("$.data[*]")
            .pagination(PaginationStyle::Cursor {
                next_token_path: "$.meta.next_cursor".into(),
                param_name: "cursor".into(),
            })
            .max_pages(50),
    )?;

    let users: Vec<serde_json::Value> = stream.fetch_all().await?;
    println!("Fetched {} users", users.len());
    Ok(())
}
```

### Page-number pagination with API key

```rust
use faucet_stream::{RestStream, RestStreamConfig, Auth, PaginationStyle};

let stream = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/v2/orders")
        .auth(Auth::ApiKey {
            header: "X-Api-Key".into(),
            value: "secret".into(),
        })
        .records_path("$.results[*]")
        .pagination(PaginationStyle::PageNumber {
            param_name: "page".into(),
            start_page: 1,
            page_size: Some(100),
            page_size_param: Some("per_page".into()),
        }),
)?;
```

### Offset pagination with Basic auth

```rust
use faucet_stream::{RestStream, RestStreamConfig, Auth, PaginationStyle};
use std::time::Duration;

let stream = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/records")
        .auth(Auth::Basic {
            username: "user".into(),
            password: "pass".into(),
        })
        .records_path("$.items[*]")
        .pagination(PaginationStyle::Offset {
            offset_param: "offset".into(),
            limit_param: "limit".into(),
            limit: 50,
            total_path: Some("$.total_count".into()),
        })
        .request_delay(Duration::from_millis(200)),
)?;
```

### OAuth2 client credentials

```rust
use faucet_stream::{Auth, fetch_oauth2_token};

let token = fetch_oauth2_token(
    "https://auth.example.com/oauth/token",
    "client-id",
    "client-secret",
    &["read:data".into()],
).await?;

let config = RestStreamConfig::new("https://api.example.com", "/data")
    .auth(Auth::Bearer(token));
```

### Streaming page-by-page

Process records as each page arrives without waiting for all pages to complete:

```rust
use faucet_stream::{RestStream, RestStreamConfig, PaginationStyle};
use futures::StreamExt;

let stream = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/v1/events")
        .records_path("$.events[*]")
        .pagination(PaginationStyle::Cursor {
            next_token_path: "$.next_cursor".into(),
            param_name: "cursor".into(),
        }),
)?;

let mut pages = stream.stream_pages();
while let Some(result) = pages.next().await {
    let records = result?;
    println!("processing page of {} records", records.len());
}
```

### Typed deserialization

```rust
use serde::Deserialize;
use faucet_stream::{RestStream, RestStreamConfig};

#[derive(Debug, Deserialize)]
struct User {
    id: u64,
    name: String,
    email: String,
}

let stream = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/users")
        .records_path("$.data[*]"),
)?;

let users: Vec<User> = stream.fetch_all_as::<User>().await?;
```

### Record transforms

Transform every record as it's extracted. Built-in transforms are feature-gated (all enabled by default):

```rust
use faucet_stream::{RestStream, RestStreamConfig, RecordTransform};

let stream = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/data")
        .records_path("$.results[*]")
        // Flatten nested objects: {"user": {"id": 1}} -> {"user__id": 1}
        .add_transform(RecordTransform::Flatten { separator: "__".into() })
        // Convert all keys to snake_case
        .add_transform(RecordTransform::KeysToSnakeCase)
        // Regex-based key renaming
        .add_transform(RecordTransform::RenameKeys {
            pattern: r"^_sdc_".into(),
            replacement: "".into(),
        })
        // Custom closure
        .add_transform(RecordTransform::custom(|mut record| {
            if let serde_json::Value::Object(ref mut map) = record {
                map.insert("_source".to_string(), serde_json::json!("my-api"));
            }
            record
        })),
)?;
```

Disable transforms you don't need:

```toml
[dependencies]
faucet-stream = { version = "0.1", default-features = false, features = ["transform-flatten"] }
```

### Schema inference

Automatically derive a JSON Schema from sampled records:

```rust
use faucet_stream::{RestStream, RestStreamConfig};

let stream = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/users")
        .records_path("$.data[*]")
        .schema_sample_size(50),  // sample up to 50 records (default: 100)
)?;

let schema = stream.infer_schema().await?;
// Returns a JSON Schema object with inferred types, nullable fields, etc.
```

### Incremental replication

Only fetch records newer than a stored bookmark:

```rust
use faucet_stream::{RestStream, RestStreamConfig, ReplicationMethod};
use serde_json::json;

let stream = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/events")
        .records_path("$.data[*]")
        .replication_method(ReplicationMethod::Incremental)
        .replication_key("updated_at")
        .start_replication_value(json!("2024-06-01T00:00:00Z")),
)?;

// fetch_all_incremental returns records + the new bookmark to persist
let (records, bookmark) = stream.fetch_all_incremental().await?;
// Save `bookmark` for the next run
```

### Partitions

Run the same stream config across multiple contexts:

```rust
use faucet_stream::{RestStream, RestStreamConfig};
use serde_json::json;
use std::collections::HashMap;

let stream = RestStream::new(
    RestStreamConfig::new("https://api.github.com", "/orgs/{org}/repos")
        .records_path("$[*]")
        .add_partition(HashMap::from([("org".into(), json!("rust-lang"))]))
        .add_partition(HashMap::from([("org".into(), json!("tokio-rs"))])),
)?;

// Fetches repos for both orgs and concatenates the results
let repos = stream.fetch_all().await?;
```

## Authentication Methods

| Method | Description |
|--------|-------------|
| `Bearer` | `Authorization: Bearer <token>` header |
| `Basic` | `Authorization: Basic <base64>` header |
| `ApiKey` | Custom header (e.g. `X-Api-Key: secret`) |
| `ApiKeyQuery` | API key as a query parameter (e.g. `?api_key=secret`) |
| `OAuth2` | Client credentials flow with automatic token caching and refresh |
| `Custom` | Arbitrary headers |

## Pagination Styles

| Style | Use When |
|-------|----------|
| `Cursor` | API returns a next-page token in the response body |
| `PageNumber` | API uses `?page=1&per_page=100` style |
| `Offset` | API uses `?offset=0&limit=50` style |
| `LinkHeader` | API returns pagination in `Link` HTTP header (GitHub-style) |
| `NextLinkInBody` | API returns the full next-page URL in the response body |

All pagination styles include loop detection — if the same cursor or link is returned twice in a row, pagination stops automatically.

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `transform-flatten` | yes | `RecordTransform::Flatten` — flatten nested objects |
| `transform-rename-keys` | yes | `RecordTransform::RenameKeys` — regex key renaming (pulls in `regex`) |
| `transform-snake-case` | yes | `RecordTransform::KeysToSnakeCase` — Meltano-compatible snake_case (pulls in `regex`) |
| `transforms` | no | Convenience: enables all three transform features |

`RecordTransform::Custom` is always available regardless of feature flags.

## Project Structure

```
src/
  lib.rs              — library entry point and re-exports
  config.rs           — RestStreamConfig with fluent builder API
  stream.rs           — RestStream: main executor (fetch_all, stream_pages, infer_schema)
  error.rs            — FaucetError enum
  auth/
    mod.rs            — Auth enum
    bearer.rs         — Bearer token auth
    basic.rs          — HTTP Basic auth
    api_key.rs        — API key header auth
    custom.rs         — Custom header auth
    oauth2.rs         — OAuth2 client credentials with token caching
  pagination/
    mod.rs            — PaginationStyle enum and PaginationState
    cursor.rs         — Cursor/token-based pagination
    page.rs           — Page number pagination
    offset.rs         — Offset/limit pagination
    link_header.rs    — Link header pagination
    next_link_body.rs — Next-link-in-body pagination
  extract/            — JSONPath record extraction
  retry/              — Exponential backoff retry executor
  replication.rs      — Incremental replication (filtering + bookmarking)
  schema.rs           — JSON Schema inference from record samples
  transform.rs        — Record transform pipeline (flatten, rename, snake_case, custom)
tests/
  auth_test.rs        — Auth integration tests
  pagination_test.rs  — Pagination integration tests
  stream_test.rs      — Stream integration tests
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
