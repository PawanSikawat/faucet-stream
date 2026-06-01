# faucet-source-rest

[![Crates.io](https://img.shields.io/crates/v/faucet-source-rest.svg)](https://crates.io/crates/faucet-source-rest)
[![Docs.rs](https://docs.rs/faucet-source-rest/badge.svg)](https://docs.rs/faucet-source-rest)

A declarative, config-driven REST API client with pluggable authentication, pagination, schema inference, and incremental replication. Attach transforms by wrapping the source with [`faucet_core::TransformingSource`](https://docs.rs/faucet-core/latest/faucet_core/struct.TransformingSource.html).

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```toml
[dependencies]
faucet-source-rest = "1.0"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:
```toml
faucet-stream = { version = "1.0", features = ["source-rest"] }
```

## Quick Start

```rust
use faucet_source_rest::{RestStream, RestStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RestStreamConfig::new("https://api.example.com", "/users")
        .max_pages(5);

    let stream = RestStream::new(config)?;
    let records = stream.fetch_all().await?;

    for record in &records {
        println!("{}", record);
    }
    Ok(())
}
```

## Configuration

### Core Request Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_url` | `String` | `""` | Base URL of the API (trailing slash is trimmed) |
| `path` | `String` | `""` | URL path relative to `base_url`. Supports `{key}` placeholders for partition substitution (e.g. `"/orgs/{org_id}/users"`) |
| `method` | `Method` | `GET` | HTTP method for the request |
| `auth` | `Auth` | `Auth::None` | Authentication method (see Authentication section) |
| `headers` | `HeaderMap` | empty | Additional HTTP headers to include in every request |
| `query_params` | `HashMap<String, String>` | empty | Query parameters to include in every request |
| `body` | `Option<Value>` | `None` | JSON request body (sent with `Content-Type: application/json`) |

### Pagination Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `pagination` | `PaginationStyle` | `PaginationStyle::None` | Pagination strategy (see Pagination section) |
| `records_path` | `Option<String>` | `None` | JSONPath expression to extract records from the response body |
| `max_pages` | `Option<usize>` | `Some(100)` | Maximum number of pages to fetch. Acts as a hard cap across all pagination styles |
| `request_delay` | `Option<Duration>` | `None` | Delay between consecutive page requests |

### Reliability Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `timeout` | `Option<Duration>` | `Some(30s)` | HTTP request timeout per individual request |
| `max_retries` | `u32` | `3` | Maximum number of retries on transient failures |
| `retry_backoff` | `Duration` | `1s` | Base duration for exponential backoff between retries. The per-attempt sleep is `retry_backoff × 2^attempt`, **capped at 60s** and scaled by random jitter in `[0.5, 1.5)` (decorrelated across concurrent retries) to avoid a thundering herd. On `429`, the server's `Retry-After` (delta-seconds **or** an RFC 7231 HTTP-date) is honored instead. |
| `tolerated_http_errors` | `Vec<u16>` | `[]` | HTTP status codes treated as an empty page **on the first request only** (i.e. a legitimately absent/empty resource). Mid-pagination a tolerated status is surfaced as an error instead of silently ending the stream — otherwise a transient failure on page _N_ would drop every later page as a "successful" run. Only safe for genuinely-empty resources. |

A **`204 No Content`** response — or any `2xx` with an empty / whitespace-only
body — is treated as an empty page ("no data"), not a parse error. A non-empty
body that isn't valid JSON still fails loudly with `FaucetError::Json`.

### Replication Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `replication_method` | `ReplicationMethod` | `FullTable` | `FullTable` fetches all records; `Incremental` filters by bookmark |
| `replication_key` | `Option<String>` | `None` | Field name used for incremental replication bookmarking |
| `start_replication_value` | `Option<Value>` | `None` | Bookmark value; records at or before this value are filtered out in incremental mode |

### Singer / Meltano Metadata Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `Option<String>` | `None` | Human-readable stream name (used in logging and Singer SCHEMA messages) |
| `primary_keys` | `Vec<String>` | `[]` | Field names that uniquely identify a record (Singer `key_properties`) |
| `schema` | `Option<Value>` | `None` | JSON Schema describing the structure of each record |
| `schema_sample_size` | `usize` | `100` | Maximum number of records to sample when inferring schema. `0` means sample all available records |

### Partition Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `partitions` | `Vec<HashMap<String, Value>>` | `[]` | Each entry is a context map substituted into `path` placeholders. The stream executes once per partition and concatenates results. Empty means run once with no substitution |
| `partition_concurrency` | `Option<usize>` | `None` | Maximum number of partitions to fetch concurrently. `None` means sequential processing |

### Authentication

The `Auth` enum supports the following strategies:

| Variant | Fields | Description |
|---------|--------|-------------|
| `None` | -- | No authentication |
| `Bearer { token }` | `String` | Bearer token in the `Authorization` header |
| `Basic { username, password }` | `String`, `String` | HTTP Basic authentication |
| `ApiKey { header, value }` | `String`, `String` | API key sent in a custom request header |
| `ApiKeyQuery { param, value }` | `String`, `String` | API key sent as a query parameter (e.g. `?api_key=secret`) |
| `OAuth2 { token_url, client_id, client_secret, scopes, expiry_ratio }` | see below | Client credentials OAuth2 flow with token caching |
| `TokenEndpoint { url, method, headers, body, token_path, expiry_path, expiry_ratio, response_validator }` | see below | Fetch token from an arbitrary HTTP endpoint |
| `Custom { headers }` | `HashMap<String, String>` | Arbitrary custom headers attached to every request |

**OAuth2 fields:**
- `token_url` (`String`): Token endpoint URL
- `client_id` (`String`): OAuth2 client ID
- `client_secret` (`String`): OAuth2 client secret
- `scopes` (`Vec<String>`): Requested scopes
- `expiry_ratio` (`f64`): Fraction of `expires_in` after which the token is refreshed. Must be in `(0.0, 1.0]`. Defaults to `0.9`

**TokenEndpoint fields:**
- `url` (`String`): Token endpoint URL
- `method` (`Method`): HTTP method (e.g. POST, GET)
- `headers` (`HeaderMap`): Headers for the token request
- `body` (`Option<Value>`): Optional JSON body for the token request
- `token_path` (`String`): JSONPath expression to extract the token from the response
- `expiry_path` (`Option<String>`): JSONPath to extract expiry (seconds) from the response
- `expiry_ratio` (`f64`): Proactive refresh ratio. Defaults to `0.9`
- `response_validator` (`Option<ResponseValidator>`): Custom success check for the token response

### Pagination

The `PaginationStyle` enum supports:

| Style | Fields | Stops When |
|-------|--------|------------|
| `None` | -- | After first page |
| `Cursor { next_token_path, param_name }` | JSONPath to next token, query param name | Next token is null or absent, or loop detected |
| `LinkHeader` | -- | No `rel="next"` in the `Link` response header |
| `NextLinkInBody { next_link_path }` | JSONPath to next page URL | URL is absent, null, or empty |
| `PageNumber { param_name, start_page, page_size, page_size_param }` | Query param name, starting page number, optional page size and its param name | Response returns zero records, or the same page body is returned twice in a row |
| `Offset { offset_param, limit_param, limit, total_path }` | Offset param, limit param, records per page, optional JSONPath to total count | A zero-record page, offset reaches total (via JSONPath), or fewer records than limit |

`Cursor`, `LinkHeader`, and `NextLinkInBody` include loop detection -- if the same cursor/link is returned twice in a row, pagination stops. `Offset` stops on a zero-record page (so a stalled offset cannot loop forever). `PageNumber` stops when a page returns zero records, and additionally detects content stagnation -- if an API clamps an out-of-range page to the last page and re-returns the identical body, pagination stops rather than looping to `max_pages`. For all styles, `max_pages` is a hard cap.

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_rest::RestStreamConfig;

// From JSON file
let config: RestStreamConfig = load_json("config.json")?;

// From .env file + environment variables
let config: RestStreamConfig = load_env_file(".env", "REST")?;
```

### Example JSON config

```json
{
  "base_url": "https://api.github.com",
  "path": "/repos/PawanSikawat/faucet-stream/issues",
  "method": "GET",
  "auth": {
    "type": "bearer",
    "config": {
      "token": "ghp_xxxxxxxxxxxx"
    }
  },
  "query_params": {
    "state": "open",
    "per_page": "100"
  },
  "pagination": {
    "type": "LinkHeader"
  },
  "records_path": "$[*]",
  "max_pages": 10,
  "timeout": 30,
  "max_retries": 3,
  "retry_backoff": 1,
  "tolerated_http_errors": [],
  "replication_method": "FullTable",
  "primary_keys": ["id"],
  "schema_sample_size": 100
}
```

### Example .env file

```env
REST_BASE_URL=https://api.github.com
REST_PATH=/repos/PawanSikawat/faucet-stream/issues
REST_METHOD=GET
REST_MAX_PAGES=10
REST_TIMEOUT=30
REST_MAX_RETRIES=3
REST_RETRY_BACKOFF=1
REST_SCHEMA_SAMPLE_SIZE=100
```

## Config Schema Introspection

```rust
use faucet_core::Source;

let stream = RestStream::new(config)?;
let schema = stream.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Methods

| Method | Return Type | Description |
|--------|------------|-------------|
| `RestStream::new(config)` | `Result<Self, FaucetError>` | Create a new stream. Validates auth at construction time |
| `fetch_all()` | `Result<Vec<Value>, FaucetError>` | Fetch all records across all pages and partitions |
| `fetch_all_as::<T>()` | `Result<Vec<T>, FaucetError>` | Fetch all records and deserialize into typed structs |
| `fetch_all_incremental()` | `Result<(Vec<Value>, Option<Value>), FaucetError>` | Fetch records with incremental replication, returning records and bookmark |
| `infer_schema()` | `Result<Value, FaucetError>` | Infer a JSON Schema from sampled records (or return the configured schema) |
| `stream_pages()` | `Pin<Box<dyn Stream<Item = Result<Vec<Value>, FaucetError>>>>` | Stream records page-by-page without waiting for all pages |

`RestStream` also implements the `faucet_core::Source::stream_pages` trait method, which is what `Pipeline::run` drives internally for memory-bounded streaming. The inherent `stream_pages()` method (which yields `Vec<Value>` pages) remains for direct callers who do not need per-page bookmarks.

## Examples

### Cursor-paginated API with bearer auth

```rust
use faucet_source_rest::{RestStream, RestStreamConfig, Auth, PaginationStyle};

let config = RestStreamConfig::new("https://api.example.com", "/v2/contacts")
    .auth(Auth::Bearer {
        token: "your-api-token".into(),
    })
    .pagination(PaginationStyle::Cursor {
        next_token_path: "$.meta.next_cursor".into(),
        param_name: "cursor".into(),
    })
    .records_path("$.data[*]")
    .max_pages(50);

let stream = RestStream::new(config)?;
let contacts = stream.fetch_all().await?;
```

### OAuth2 client credentials with incremental replication

```rust
use faucet_source_rest::*;
use serde_json::json;

let config = RestStreamConfig::new("https://api.example.com", "/v1/events")
    .auth(Auth::OAuth2 {
        token_url: "https://auth.example.com/oauth/token".into(),
        client_id: "my-client-id".into(),
        client_secret: "my-client-secret".into(),
        scopes: vec!["read:events".into()],
        expiry_ratio: 0.9,
    })
    .pagination(PaginationStyle::Offset {
        offset_param: "offset".into(),
        limit_param: "limit".into(),
        limit: 100,
        total_path: Some("$.total".into()),
    })
    .records_path("$.events[*]")
    .replication_method(ReplicationMethod::Incremental)
    .replication_key("updated_at")
    .start_replication_value(json!("2025-01-01T00:00:00Z"));

let stream = RestStream::new(config)?;
let (records, bookmark) = stream.fetch_all_incremental().await?;
println!("Fetched {} records, new bookmark: {:?}", records.len(), bookmark);
```

### Multi-partition concurrent fetch

```rust
use faucet_source_rest::{RestStream, RestStreamConfig, Auth};
use std::collections::HashMap;
use serde_json::json;

let config = RestStreamConfig::new("https://api.example.com", "/orgs/{org_id}/members")
    .auth(Auth::Bearer {
        token: "token".into(),
    })
    .add_partition(HashMap::from([("org_id".into(), json!("acme"))]))
    .add_partition(HashMap::from([("org_id".into(), json!("globex"))]))
    .add_partition(HashMap::from([("org_id".into(), json!("initech"))]))
    .partition_concurrency(Some(3))
    .records_path("$.members[*]");

let stream = RestStream::new(config)?;
let all_members = stream.fetch_all().await?;
```

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `transform-flatten` | yes | Enable the `Flatten` record transform |
| `transform-rename-keys` | yes | Enable the `RenameKeys` regex-based transform |
| `transform-keys-case` | yes | Enable the `KeysCase` transform (snake / camel / pascal / kebab / screaming_snake) |
| `transform-select` | no | Enable the `Select` transform (keep listed top-level fields) |
| `transform-drop` | no | Enable the `Drop` transform (remove listed top-level fields) |
| `transform-set` | no | Enable the `Set` transform (insert/overwrite constants) |
| `transform-rename-field` | no | Enable the `RenameField` transform (exact-name rename) |
| `transform-cast` | no | Enable the `Cast` transform (per-field type coercion with `on_error` policy) |
| `transform-redact` | no | Enable the `Redact` transform (mask listed field values) |
| `transform-value-case` | no | Enable the `ValueCase` transform (lower / upper / trim string values) |
| `transform-spell-symbols` | no | Enable the `SpellSymbols` transform (spell out `%`, `#`, `$`, … in keys) |
| `transforms` | no | Enable every transform feature |

## License

Licensed under MIT or Apache-2.0.
