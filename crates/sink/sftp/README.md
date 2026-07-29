# faucet-sink-sftp

SFTP sink connector for the [faucet-stream](https://crates.io/crates/faucet-stream)
ecosystem.

Writes records to an SFTP server as JSON Lines objects under a remote
directory. Append-only.

## Atomic writes

Each object is uploaded to a hidden temporary name (`<uuid>.jsonl.tmp`) and then
**renamed** to its final name (`<uuid>.jsonl`). A consumer watching the
directory therefore never observes a partially-written file — a downstream
reader either sees the complete object or does not see it at all.

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
| `path` | string | — | Remote directory prefix under which objects are written. |
| `file_extension` | string | `.jsonl` | Extension for written objects. |
| `batch_size` | integer | `1000` | Records per object; `0` = one object per `write_batch` call. |

The sink opens the SSH connection lazily on the first write and reuses it. It
attempts to create the target directory on first connect (best-effort).

## Example

```yaml
version: 1
pipeline:
  source:
    kind: stdout   # replace with a real source
    config: {}
  sink:
    kind: sftp
    config:
      host: sftp.example.com
      username: uploader
      type: private_key
      config:
        path: /home/uploader/.ssh/id_ed25519
      known_hosts:
        mode: strict
      path: /incoming/events
      file_extension: .jsonl
      batch_size: 0
```

Licensed under MIT OR Apache-2.0.
