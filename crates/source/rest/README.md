# faucet-source-rest

[![Crates.io](https://img.shields.io/crates/v/faucet-source-rest.svg)](https://crates.io/crates/faucet-source-rest)
[![Docs.rs](https://docs.rs/faucet-source-rest/badge.svg)](https://docs.rs/faucet-source-rest)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-rest.svg)](https://github.com/PawanSikawat/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-rest.svg)](https://github.com/PawanSikawat/faucet-stream#license)

A declarative, config-driven **REST API source** with pluggable authentication, six pagination styles, schema inference, and incremental replication. Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

This is the flagship faucet-stream source: point it at any JSON-over-HTTP API, describe how to authenticate and paginate, and it streams every record page-by-page into any faucet-stream sink — with retries, `Retry-After` honouring, loop detection, and resumable bookmarks — all from one YAML config and no glue code.

## Feature highlights

- **Six pagination styles** — `Cursor`, `LinkHeader`, `NextLinkInBody`, `PageNumber`, `Offset`, and `None`, each with its own termination/loop guard so a misbehaving API can't loop forever.
- **Eight auth methods** — `bearer`, `basic`, `api_key` (header), `api_key_query`, `oauth2` (client credentials with token caching), `token_endpoint` (fetch a token from an arbitrary endpoint), `custom` headers, and `none` — plus shared `auth: { ref }` providers via the CLI's top-level `auth:` catalog.
- **Memory-bounded streaming** — overrides `Source::stream_pages`, so `Pipeline::run` writes each page to the sink as it arrives; peak memory stays `O(page)` regardless of total record count.
- **Resilient by default** — exponential backoff with jitter (capped at 60 s), `429` `Retry-After` (delta-seconds or HTTP-date) honouring, and a `tolerated_http_errors` allowlist for legitimately-absent resources.
- **Incremental replication** — bookmark by any record field, persist it across runs with a state store, and resume from the last value.
- **Concurrent partitions** — fan a single config across many path substitutions (`/orgs/{org_id}/users`) and fetch them concurrently.
- **Schema inference** — sample records to produce a JSON Schema, or supply your own; Singer/Meltano `primary_keys` / `name` metadata is carried through.
- **Client built once** — the `reqwest` client is constructed in `new()` and reused for every request and partition.

## Installation

```bash
# As a library:
cargo add faucet-source-rest
cargo add tokio --features full

# In the CLI (source-rest is a DEFAULT feature — already enabled):
cargo install faucet-cli
```

The umbrella crate enables it by default too:

```bash
cargo add faucet-stream --features source-rest
```

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
name: github_issues_to_jsonl

pipeline:
  source:
    type: rest
    config:
      base_url: https://api.github.com
      path: /repos/PawanSikawat/faucet-stream/issues
      method: GET
      auth:
        type: bearer
        config:
          token: ${env:GITHUB_TOKEN}
      query_params:
        state: open
        per_page: "100"
      pagination:
        type: LinkHeader
  sink:
    type: jsonl
    config:
      path: ./out/issues.jsonl
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core request

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_url` | string | `""` | Base URL of the API (trailing slash trimmed). |
| `path` | string | `""` | URL path relative to `base_url`. Supports `{key}` placeholders for partition substitution (e.g. `/orgs/{org_id}/users`). |
| `method` | string | `GET` | HTTP method for the request. |
| `auth` | `Auth` / `{ ref }` | `none` | Inline `{ type, config }` auth, or a `{ ref: <name> }` pointer to a shared provider. See [Authentication](#authentication). |
| `headers` | map | empty | Extra HTTP headers sent on every request (library/`header()` only; not serialized in YAML/JSON). |
| `query_params` | map<string,string> | empty | Query parameters added to every request. |
| `body` | JSON / null | `null` | JSON request body (sent with `Content-Type: application/json`). |

### Pagination

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `pagination` | `PaginationStyle` | `None` | Pagination strategy. See [Pagination](#pagination). |
| `records_path` | string / null | `null` | JSONPath expression to extract the record array from each response body (e.g. `$.data[*]`). When unset, the whole body is treated as the record set. |
| `max_pages` | int / null | `100` | Hard cap on pages fetched, across **all** pagination styles. `null` removes the cap (rely on the style's own termination). |
| `request_delay` | int (seconds) / null | `null` | Delay between consecutive page requests. |

### Reliability

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `timeout` | int (seconds) / null | `30` | Per-request HTTP timeout. |
| `max_retries` | int | `3` | Max retries on transient failures. |
| `retry_backoff` | int (seconds) | `1` | Base for exponential backoff. Per-attempt sleep is `retry_backoff × 2^attempt`, **capped at 60 s** and scaled by random jitter in `[0.5, 1.5)` (decorrelated across concurrent retries). On `429`, the server's `Retry-After` (delta-seconds **or** an RFC 7231 HTTP-date) is honoured instead. |
| `tolerated_http_errors` | array<int> | `[]` | HTTP status codes treated as an empty page **on the first request only**. Mid-pagination, a tolerated status surfaces as an error instead of silently ending the stream (otherwise a transient failure on page _N_ would drop every later page as a "successful" run). Only safe for genuinely-empty resources. |

A **`204 No Content`** response — or any `2xx` with an empty/whitespace-only body — is treated as an empty page ("no data"), not a parse error. A non-empty body that isn't valid JSON still fails loudly with `FaucetError::Json`.

#### Unified `resilience:` policy

When driven by the CLI, a pipeline-level [`resilience:`](https://pawansikawat.github.io/faucet-stream/cookbook/resilience.html) block can inject one shared retry policy into this source. **Legacy fields win when set explicitly:** if you set `max_retries` or `retry_backoff` to anything other than their defaults (`3` / `1`), the per-connector value is used and the injected policy is ignored for that field — an explicit setting is never silently overridden. Otherwise the injected policy applies.

Because the REST source keeps its own `429`/`Retry-After`-aware retry runner, it honors **only** the injected policy's `max_attempts` (→ `max_retries`) and `base` (→ `retry_backoff`). The policy's `retry_on`, `max` (per-sleep cap), and `jitter` fields are **inert on REST** — they are honored on the `xml`/`graphql` sources and on every sink-side write.

### Replication & state

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `replication_method` | `{ type: FullTable \| Incremental }` | `FullTable` | `FullTable` fetches all records; `Incremental` filters by bookmark. |
| `replication_key` | string / null | `null` | Record **field name** (not a JSONPath) used for incremental bookmarking. |
| `start_replication_value` | JSON / null | `null` | Bookmark value; records where `record[replication_key] <= start_replication_value` are filtered out in `Incremental` mode. |
| `state_key` | string / null | `null` | Stable key used by `Pipeline::with_state_store` to persist this stream's bookmark across runs. See [Resume & state](#resume--state). |

### Singer / Meltano metadata

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string / null | `null` | Human-readable stream name (logging, Singer SCHEMA messages). |
| `primary_keys` | array<string> | `[]` | Fields that uniquely identify a record (Singer `key_properties`). |
| `schema` | JSON / null | `null` | JSON Schema describing each record. When set, it's returned by `infer_schema()` instead of sampling. |
| `schema_sample_size` | int | `100` | Max records sampled when inferring the schema. `0` = sample all available records. |

### Partitions

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `partitions` | array<map> | `[]` | Each entry is a context map substituted into `path` placeholders. The stream runs once per partition and concatenates results. Empty = run once with no substitution. |
| `partition_concurrency` | int / null | `null` | Max partitions fetched concurrently. `null` = sequential. |

## Authentication

The `auth` field accepts the project-wide adjacently-tagged `{ type, config }` shape (snake_case discriminators), or a `{ ref: <name> }` pointer into the CLI's top-level shared `auth:` catalog.

| `type` | `config` fields | Description |
|--------|-----------------|-------------|
| `none` | *(none)* | No authentication. |
| `bearer` | `token` | Bearer token in the `Authorization` header. |
| `basic` | `username`, `password` | HTTP Basic authentication. |
| `api_key` | `header`, `value` | API key sent in a custom request header. |
| `api_key_query` | `param`, `value` | API key sent as a query parameter (e.g. `?api_key=secret`). |
| `oauth2` | `token_url`, `client_id`, `client_secret`, `scopes`, `expiry_ratio` | OAuth2 client-credentials flow with token caching. |
| `token_endpoint` | `url`, `method`, `body`, `token_path`, `expiry_path`, `expiry_ratio` | Fetch a token from an arbitrary HTTP endpoint (JSONPath-extracted). |
| `custom` | `headers` (map<string,string>) | Arbitrary headers attached to every request. |

**`oauth2` / `token_endpoint` notes:** `expiry_ratio` is the fraction of the token lifetime after which the cached token is proactively refreshed — must be in `(0.0, 1.0]`, defaults to `0.9`. For `token_endpoint`, `token_path` is the JSONPath to the token string and `expiry_path` (optional) is the JSONPath to the expiry in seconds; when absent the token is cached indefinitely.

```yaml
# Bearer
auth:
  type: bearer
  config:
    token: ${env:GITHUB_TOKEN}
```

```yaml
# API key header
auth:
  type: api_key
  config:
    header: X-API-Key
    value: ${env:API_KEY}
```

```yaml
# OAuth2 client credentials
auth:
  type: oauth2
  config:
    token_url: https://auth.example.com/oauth/token
    client_id: ${env:CLIENT_ID}
    client_secret: ${env:CLIENT_SECRET}
    scopes: ["read:events"]
    expiry_ratio: 0.9
```

```yaml
# Shared provider from the top-level auth: catalog
auth: { ref: my_idp }
```

## Examples

### Cursor-paginated API with bearer auth

```yaml
source:
  type: rest
  config:
    base_url: https://api.example.com
    path: /v2/contacts
    auth:
      type: bearer
      config:
        token: ${env:API_TOKEN}
    pagination:
      type: Cursor
      next_token_path: $.meta.next_cursor
      param_name: cursor
    records_path: $.data[*]
    max_pages: 50
```

### OAuth2 + incremental replication with a persisted bookmark

```yaml
version: 1
name: events_incremental
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      path: /v1/events
      auth:
        type: oauth2
        config:
          token_url: https://auth.example.com/oauth/token
          client_id: ${env:CLIENT_ID}
          client_secret: ${env:CLIENT_SECRET}
          scopes: ["read:events"]
          expiry_ratio: 0.9
      pagination:
        type: Offset
        offset_param: offset
        limit_param: limit
        limit: 100
        total_path: $.total
      records_path: $.events[*]
      replication_method:
        type: Incremental
      replication_key: updated_at
      start_replication_value: "2026-01-01T00:00:00Z"
      state_key: events_stream
  sink:
    type: jsonl
    config:
      path: ./out/events.jsonl
  state:
    type: file
    config:
      path: ./state.json
```

### Multi-partition concurrent fetch

```yaml
source:
  type: rest
  config:
    base_url: https://api.example.com
    path: /orgs/{org_id}/members
    auth:
      type: bearer
      config:
        token: ${env:API_TOKEN}
    records_path: $.members[*]
    partitions:
      - { org_id: acme }
      - { org_id: globex }
      - { org_id: initech }
    partition_concurrency: 3
```

### Tolerating a `429` and capping retries

```yaml
source:
  type: rest
  config:
    base_url: https://api.example.com
    path: /v1/comments
    auth:
      type: api_key
      config:
        header: X-API-Key
        value: ${env:API_KEY}
    pagination:
      type: LinkHeader
    records_path: $.items[*]
    timeout: 60
    max_retries: 5
    retry_backoff: 2
    tolerated_http_errors: [429]
```

## Pagination

The `pagination` field selects a `PaginationStyle` (tagged by `type`). `max_pages` is a hard cap across all styles.

| Style (`type`) | Fields | Stops when |
|----------------|--------|------------|
| `None` | — | After the first page. |
| `Cursor` | `next_token_path`, `param_name` | Next-token JSONPath is null/absent, or the same cursor repeats (loop detection). |
| `LinkHeader` | — | No `rel="next"` in the `Link` response header, or the same link repeats. |
| `NextLinkInBody` | `next_link_path` | Next-page URL is absent, null, empty, or repeats. |
| `PageNumber` | `param_name`, `start_page`, `page_size`, `page_size_param` | A zero-record page, or the same body returned twice in a row (content-stagnation detection for APIs that clamp out-of-range pages). |
| `Offset` | `offset_param`, `limit_param`, `limit`, `total_path` | A zero-record page, offset reaches `total` (via `total_path`), or a page returns fewer records than `limit`. |

## Streaming & batching

`RestStream` overrides `Source::stream_pages`, fetching the next HTTP page on demand and yielding it as a `StreamPage`. `Pipeline::run` writes each page to the sink as it arrives, so peak memory is bounded at one page no matter how large the feed. The bookmark is carried on the final page (incremental mode) so the state store advances only after the sink confirms the full run.

The inherent `stream_pages()` method (yielding `Vec<Value>` pages, no per-page bookmark) remains for direct callers, alongside the eager `fetch_all()` / `fetch_all_incremental()` helpers.

## Resume & state

This source supports resumable runs. Set `state_key` and configure a `state:` block (or call `Pipeline::with_state_store` from Rust). On each run the pipeline:

1. loads the previously persisted bookmark and applies it via `apply_start_bookmark` (overriding `start_replication_value`);
2. fetches only records newer than the bookmark (`replication_method: Incremental` + `replication_key`);
3. persists the new bookmark **only after the sink confirms** the batch — so a crash mid-run re-fetches rather than skips.

`state_key` must satisfy `faucet_core::state::validate_state_key`. See the second [example](#oauth2--incremental-replication-with-a-persisted-bookmark) above.

## Config loading & schema introspection

Configs load from YAML/JSON files or environment variables:

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_rest::RestStreamConfig;

// From a JSON file:
let config: RestStreamConfig = load_json("config.json")?;

// From a .env file + environment (REST_ prefix):
let config: RestStreamConfig = load_env_file(".env", "REST")?;
```

Example `.env`:

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

Inspect the full JSON Schema with:

```bash
faucet schema source rest
```

## Library usage

```rust
use faucet_source_rest::{RestStream, RestStreamConfig, Auth, PaginationStyle};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RestStreamConfig::new("https://api.example.com", "/v2/contacts")
        .auth(Auth::Bearer { token: "your-api-token".into() })
        .pagination(PaginationStyle::Cursor {
            next_token_path: "$.meta.next_cursor".into(),
            param_name: "cursor".into(),
        })
        .records_path("$.data[*]")
        .max_pages(50);

    let stream = RestStream::new(config)?;          // validates auth at construction
    let contacts = stream.fetch_all().await?;
    println!("fetched {} records", contacts.len());
    Ok(())
}
```

Inherent helper methods on `RestStream`:

| Method | Returns | Description |
|--------|---------|-------------|
| `RestStream::new(config)` | `Result<Self, FaucetError>` | Build the stream; validates auth at construction time. |
| `fetch_all()` | `Result<Vec<Value>, _>` | Fetch all records across all pages and partitions. |
| `fetch_all_as::<T>()` | `Result<Vec<T>, _>` | Fetch and deserialize into typed structs. |
| `fetch_all_incremental()` | `Result<(Vec<Value>, Option<Value>), _>` | Fetch with incremental replication; returns records + new bookmark. |
| `infer_schema()` | `Result<Value, _>` | Infer a JSON Schema from sampled records (or return the configured `schema`). |

Attach transforms by wrapping the source with [`faucet_core::TransformingSource`](https://docs.rs/faucet-core/latest/faucet_core/struct.TransformingSource.html).

## How it works

1. `new()` resolves the auth method and builds the `reqwest` client **once**, reusing it for every request and partition.
2. For each partition, `{key}` placeholders in `path` are substituted from the context map; with `partition_concurrency` set, partitions run concurrently.
3. Each page request is wrapped in the retry layer: transient failures back off exponentially with jitter (capped at 60 s); `429` honours `Retry-After`.
4. Records are extracted from the response body via `records_path` (JSONPath); the pagination style decides the next request and when to stop.
5. In `Incremental` mode, records at or before the bookmark are filtered out and the max replication-key value becomes the new bookmark, carried on the final page.

## Lineage dataset URI

`https://<base_url><path>` (credentials stripped) — e.g. `https://api.example.com/v1/users`.

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `transform-flatten` | yes | `Flatten` record transform. |
| `transform-rename-keys` | yes | `RenameKeys` regex-based transform. |
| `transform-keys-case` | yes | `KeysCase` transform (snake / camel / pascal / kebab / screaming_snake). |
| `transform-select` | no | `Select` transform (keep listed top-level fields). |
| `transform-drop` | no | `Drop` transform (remove listed top-level fields). |
| `transform-set` | no | `Set` transform (insert/overwrite constants). |
| `transform-rename-field` | no | `RenameField` transform (exact-name rename). |
| `transform-cast` | no | `Cast` transform (per-field type coercion with `on_error` policy). |
| `transform-redact` | no | `Redact` transform (mask listed field values). |
| `transform-value-case` | no | `ValueCase` transform (lower / upper / trim string values). |
| `transform-spell-symbols` | no | `SpellSymbols` transform (spell out `%`, `#`, `$`, … in keys). |
| `transforms` | no | Enable every transform feature. |

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `401` / `403` on every page | Wrong or missing credentials. Verify the `auth` block; for `bearer`/`api_key`, confirm the token/header value is set (e.g. `${env:GITHUB_TOKEN}` is exported). |
| Auth validation fails at `RestStream::new` | An auth field is malformed — e.g. `expiry_ratio` outside `(0.0, 1.0]`, or an empty required field. Fix the value; auth is validated at construction, not first request. |
| Pagination stops after one page | `pagination` left as the default `None`. Set the style your API uses (`LinkHeader`, `Cursor`, `Offset`, …). |
| Pagination stalls / never advances | The cursor/link token repeated, or the response body was identical twice — loop detection halted it. Check `next_token_path` / `next_link_path` points at the *next* token, not the current one. |
| Records come back empty but the API has data | `records_path` doesn't match the response shape. Test your JSONPath against a real body (e.g. `$.data[*]` vs `$.items[*]`); when unset, the whole body is treated as the record set. |
| Run ends early after a transient `5xx`/`429` mid-pagination | A `tolerated_http_errors` code only short-circuits the **first** request. Mid-stream it errors instead of silently truncating — raise `max_retries` / `retry_backoff` rather than tolerating the code. |
| `FaucetError::Json` on a non-empty body | The response wasn't valid JSON (HTML error page, gateway response). Empty/`204` bodies are fine; a non-empty non-JSON body fails loudly by design. |
| Incremental run re-fetches everything | No `state_key` + `state:` block, so the bookmark isn't persisted; or `replication_method` is still `FullTable`. Set both, plus `replication_key`. |
| Rate-limited despite retries | The API returns `429` without `Retry-After`, or limits are stricter than backoff. Add `request_delay` to space requests, and/or lower `partition_concurrency`. |

## See also

- [Connector catalog & capability matrix](https://pawansikawat.github.io/faucet-stream/reference/connectors.html)
- [Authentication cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/auth.html)
- [Pagination cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/pagination.html)
- [Resumable state & bookmarks](https://pawansikawat.github.io/faucet-stream/cookbook/state.html)
- [Config-file grammar](https://pawansikawat.github.io/faucet-stream/reference/config.html)
- Related crates: [`faucet-source-graphql`](https://crates.io/crates/faucet-source-graphql), [`faucet-source-xml`](https://crates.io/crates/faucet-source-xml), [`faucet-sink-http`](https://crates.io/crates/faucet-sink-http), [`faucet-auth`](https://crates.io/crates/faucet-auth).

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
