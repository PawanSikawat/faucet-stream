# faucet-source-http-file

Authenticated **HTTP file source** for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) data-pipeline toolkit.

Download a file from an authenticated HTTP(S) URL — a Microsoft Graph /
OneDrive / SharePoint `…/content` endpoint, a signed S3/GCS download URL, or any
authed static host — and parse it into records.

- **CSV** parsing is always available (streaming RFC-4180 via `csv-async`, so
  quoted fields with embedded newlines round-trip correctly).
- **Excel** (`.xlsx`/`.xls`) parsing is available behind the `excel` crate
  feature (via [`calamine`](https://crates.io/crates/calamine)).
- **`format: auto`** (the default) infers CSV vs Excel from the URL extension.

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | — (required) | File URL. Supports `{key}` context substitution. |
| `auth` | object | `{ type: none }` | Inline `{ type, config }` or `{ ref: <name> }` (shared provider). |
| `format` | `auto`\|`csv`\|`excel` | `auto` | `auto` infers from the URL extension. |
| `delimiter` | char | `,` | CSV field delimiter. |
| `has_headers` | bool | `true` | Whether the first CSV row supplies field names. |
| `sheet` | string | first sheet | Excel worksheet name, or a 0-based index as a string. |
| `header_row` | int | `0` | Excel: 0-based index of the header row. |
| `batch_size` | int | `1000` | Records per emitted page. `0` = one page. |
| `timeout_secs` | int | `60` | Per-request HTTP timeout. |

### Authentication

The `auth` block uses the project-wide `{ type, config }` shape:

| `type` | `config` |
|--------|----------|
| `none` | — |
| `bearer` | `{ token }` → `Authorization: Bearer <token>` |
| `basic` | `{ username, password }` |
| `api_key` | `{ header, value }` → a named header |
| `custom` | `{ headers: { … } }` → arbitrary headers |

It also accepts `auth: { ref: <name> }` to share a provider from the top-level
`auth:` catalog — so an `oauth2_refresh` provider minting a Microsoft Graph
access token can be reused across matrix rows without re-authenticating.

## Example

```yaml
pipeline:
  source:
    type: http-file
    config:
      url: https://graph.microsoft.com/v1.0/me/drive/items/ITEM_ID/content
      format: excel
      sheet: "Sheet1"
      auth:
        ref: graph            # a shared oauth2_refresh provider
  sink:
    type: jsonl
    config:
      path: ./out/records.jsonl
```

## Features

- `excel` — enable Excel parsing (`calamine`). CSV is always available.

## Library usage

```rust,no_run
use faucet_source_http_file::{HttpFileSource, HttpFileSourceConfig};

# async fn run() -> Result<(), faucet_core::FaucetError> {
let cfg = HttpFileSourceConfig::new("https://example.com/data.csv");
let source = HttpFileSource::new(cfg)?;
# let _ = source;
# Ok(()) }
```

## License

Licensed under either of Apache-2.0 or MIT at your option.
