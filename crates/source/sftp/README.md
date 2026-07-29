# faucet-source-sftp

SFTP source connector for the [faucet-stream](https://crates.io/crates/faucet-stream)
ecosystem.

Lists a remote directory (or reads a single file) over SFTP and streams the
files as JSON Lines, JSON arrays, or raw text. JSON Lines and raw text are
decoded incrementally, so memory stays bounded regardless of file size; JSON
arrays are buffered per file (the closing `]` is needed to validate the
structure) and then chunked.

Connection, authentication, and host-key verification come from
[`faucet-common-sftp`](https://crates.io/crates/faucet-common-sftp).

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | string | — | Server hostname or IP. |
| `port` | integer | `22` | Server port. |
| `username` | string | — | SSH username. |
| `type` / `config` | auth | — | `password` or `private_key` (see `faucet-common-sftp`). |
| `known_hosts` | policy | `{ mode: accept_new }` | Host-key verification policy. |
| `path` | string | — | Remote directory to list, or a single file. |
| `glob` | string | none | Filename glob (`*` / `?`) applied to basenames when `path` is a directory. |
| `format` | enum | `jsonl` | `jsonl` \| `json_array` \| `raw_text`. |
| `batch_size` | integer | `1000` | Records per page; `0` = one page per file. |

`raw_text` emits one record per file: `{ "path": <remote path>, "content": <file text> }`.

The SFTP source is not resumable — every page carries no bookmark.

## Example

```yaml
version: 1
pipeline:
  source:
    kind: sftp
    config:
      host: sftp.example.com
      port: 22
      username: reporting
      type: password
      config:
        password: ${env:SFTP_PASSWORD}
      known_hosts:
        mode: accept_new
      path: /exports/daily
      glob: "*.jsonl"
      format: jsonl
      batch_size: 1000
  sink:
    kind: stdout
    config: {}
```

Licensed under MIT OR Apache-2.0.
