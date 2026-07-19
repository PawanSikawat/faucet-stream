# faucet-source-azure-blob

Azure Blob Storage / ADLS Gen2 **source** connector for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

Lists and reads objects from an Azure blob container (or ADLS Gen2 filesystem)
and emits them as JSON records. Built on
[`object_store`](https://crates.io/crates/object_store)'s Azure backend, so both
classic Blob and ADLS Gen2 hierarchical namespaces are supported through one
code path.

## Config

Connection fields come from `faucet-common-azure` and are set at the top level:

| Field | Type | Notes |
|---|---|---|
| `container` | string | **Required.** Blob container / ADLS filesystem. |
| `account` | string | Storage-account name (optional with a connection string / emulator). |
| `auth` | `{ type, config }` | `account_key` / `sas_token` / `connection_string` / `managed_identity` / `service_principal` / `default`. |
| `endpoint` | string | Custom blob endpoint (emulator / sovereign cloud). |
| `allow_http` | bool | Permit plaintext HTTP (Azurite). |
| `use_emulator` | bool | Target the Azurite emulator. |

Source-specific fields:

| Field | Type | Default | Notes |
|---|---|---|---|
| `prefix` | string | — | Object-name prefix filter. Ignored when `object_keys` is set. |
| `object_keys` | list | — | Explicit object names; skips listing. |
| `file_format` | enum | `json_lines` | `json_lines` / `json_array` / `raw_text`. |
| `max_objects` | int | — | Hard cap on objects read. |
| `concurrency` | int | `10` | Max concurrent object reads. |
| `batch_size` | int | `1000` | Records per `StreamPage`; `0` = one page per object. |
| `compression` | enum | `auto` | `auto` / `gzip` / `zstd` (requires the `compression` feature). |

## File formats

- **`json_lines`** — one JSON record per line; streamed line-by-line (bounded memory).
- **`json_array`** — the whole object is a JSON array; buffered then chunked.
- **`raw_text`** — each object becomes one record `{ "key", "content" }`.

## Example

```yaml
pipeline:
  source:
    type: azure-blob
    config:
      container: raw
      account: mystorageacct
      auth: { type: account_key, config: { account_key: "${env:AZURE_KEY}" } }
      prefix: events/2026/
      file_format: json_lines
```

## License

MIT
