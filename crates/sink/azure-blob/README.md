# faucet-sink-azure-blob

Azure Blob Storage / ADLS Gen2 **sink** connector for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

Writes JSON records to an Azure blob container (or ADLS Gen2 filesystem) as JSON
Lines objects. Built on [`object_store`](https://crates.io/crates/object_store)'s
Azure backend, so classic Blob and ADLS Gen2 are served through one code path.

## Config

Connection fields come from `faucet-common-azure` and are set at the top level:

| Field | Type | Notes |
|---|---|---|
| `container` | string | **Required.** Blob container / ADLS filesystem (must already exist). |
| `account` | string | Storage-account name (optional with a connection string / emulator). |
| `auth` | `{ type, config }` | `account_key` / `sas_token` / `connection_string` / `managed_identity` / `service_principal` / `default`. |
| `endpoint` | string | Custom blob endpoint (emulator / sovereign cloud). |
| `allow_http` | bool | Permit plaintext HTTP (Azurite). |
| `use_emulator` | bool | Target the Azurite emulator. |

Sink-specific fields:

| Field | Type | Default | Notes |
|---|---|---|---|
| `prefix` | string | `""` | Object-name prefix; a virtual "directory" in the flat blob namespace. |
| `file_extension` | string | `.jsonl` | Extension for written objects. |
| `max_records_per_file` | int | — | Cap records per object (file rollover). |
| `concurrency` | int | `10` | Max concurrent uploads. |
| `batch_size` | int | `1000` | Records per object; `0` writes one object per `write_batch` (recommended). |
| `compression` | enum | `auto` | `auto` / `gzip` / `zstd` (requires the `compression` feature); resolved from `file_extension`. |

Object names are `{prefix}{uuidv7}{file_extension}` — time-sortable so a listing
returns objects in write order. The container is not created automatically; the
prefix is virtual (blob namespaces are flat).

## Example

```yaml
pipeline:
  sink:
    type: azure-blob
    config:
      container: exports
      account: mystorageacct
      auth: { type: account_key, config: { account_key: "${env:AZURE_KEY}" } }
      prefix: events/2026/
      batch_size: 0
```

## License

MIT
