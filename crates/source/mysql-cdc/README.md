# faucet-source-mysql-cdc

MySQL CDC (Change Data Capture) source for [`faucet-stream`](https://github.com/PawanSikawat/faucet-stream). Tails the MySQL binary log via row-based replication and emits per-row change events (insert, update, delete, and optionally DDL) as JSON records. Bookmarks are persisted as binlog file/position coordinates, so pipelines resume exactly where they left off after a restart — no duplicates, no gap.

**Targets transactional InnoDB tables.** Commits arrive as `XidEvent`; explicit `COMMIT` statements from non-transactional or mixed-engine workloads are also handled with identical durability semantics. Each committed transaction is emitted as its own `StreamPage` with a bookmark attached — uncommitted partial transactions are discarded at idle timeout and re-delivered from the last persisted bookmark on the next run.

---

## Server prerequisites

Before connecting, the MySQL server must have binary logging configured for row-based replication. All four settings are verified at startup; the connector fails with a clear error if any are missing.

### Required server variables

Set these in `my.cnf` (or pass as `--option` flags to `mysqld`):

```ini
[mysqld]
server-id          = 1          # any non-zero value, unique per server
log_bin            = mysql-bin  # enable binary logging
binlog_format      = ROW        # required: row-level events
binlog_row_image   = FULL       # required: full before/after images
binlog_row_metadata = FULL      # REQUIRED for column names in the envelope
```

**`binlog_row_metadata=FULL` is critical.** Without it, column names are absent from row events; the connector cannot decode column names and will fail. This setting is `MINIMAL` by default in MySQL 8.0 and must be explicitly changed.

### Required user grants

The replication user must have `REPLICATION SLAVE` and `REPLICATION CLIENT` on `*.*`:

```sql
CREATE USER 'repl'@'%' IDENTIFIED BY 'repl';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';
FLUSH PRIVILEGES;
```

### GTID mode (only for `start_position: gtid_set`)

If you use `start_position: { type: gtid_set, value: "…" }`, the server must also have GTID mode enabled:

```ini
gtid_mode               = ON
enforce_gtid_consistency = ON
```

File/pos and other start positions work with or without GTID mode.

---

## Quick start

```yaml
# pipeline.yaml
version: 1
source:
  type: mysql-cdc
  config:
    # No default database: the binlog is global; mysql-cdc reads all databases
    # and filters client-side via include_tables/exclude_tables.
    connection_url: mysql://repl:repl@localhost:3306
    server_id: 1001
    start_position:
      type: current
    include_columns: true
    idle_timeout: 30
sink:
  type: jsonl
  config:
    path: ./changes.jsonl
state:
  type: file
  config:
    path: ./state/mysql_cdc.json
```

```bash
faucet run pipeline.yaml
```

---

## Output record schema (CDC envelope)

Every change event is one JSON object:

```json
{
  "op": "c",
  "ts_ms": 1779019200000,
  "schema": "appdb",
  "table": "users",
  "before": null,
  "after": { "id": 1, "name": "alice", "email": "alice@example.com" },
  "lsn": { "file": "mysql-bin.000003", "pos": 4567 },
  "txid": 42
}
```

### Field reference

| Field | Type | Description |
|-------|------|-------------|
| `op` | string | Operation type: `c` (insert), `u` (update), `d` (delete). DDL events use `ddl` when `emit_schema_changes: true`. |
| `ts_ms` | number | Wall-clock time of the binlog event in Unix-epoch milliseconds (from the event header timestamp). |
| `schema` | string | The source database (schema) name. |
| `table` | string | The source table name. |
| `before` | object \| null | Pre-image of the row. Populated on updates and deletes when `include_columns: true`. `null` on inserts, or when `include_columns: false`. |
| `after` | object \| null | Post-image of the row. Populated on inserts and updates. `null` on deletes. |
| `lsn` | object | Binlog coordinates of the commit event: `{ "file": "mysql-bin.000003", "pos": 4567 }`. Used as the persisted bookmark. |
| `txid` | number | Monotonically increasing per-session transaction counter (resets to 0 on each `faucet run`). Useful for grouping rows from the same transaction. |

### op mapping

| Binlog event | `op` value |
|---|---|
| `WriteRowsEvent` (INSERT) | `c` |
| `UpdateRowsEvent` (UPDATE) | `u` |
| `DeleteRowsEvent` (DELETE) | `d` |
| DDL (`QueryEvent` other than BEGIN/COMMIT), when `emit_schema_changes: true` | `ddl` |

---

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_url` | string | — | MySQL connection URL, e.g. `mysql://repl:pass@host:3306/db`. Required. |
| `server_id` | u32 | — | Replica server ID. **Required** and must be unique across all replication clients connecting to this server. |
| `include_tables` | string[] | `[]` (all) | Client-side allowlist of `database.table` names. Empty = all tables. Must be fully qualified (e.g. `appdb.users`). |
| `exclude_tables` | string[] | `[]` | Client-side blocklist of `database.table` names. Allowlist takes precedence when both are set. Must be fully qualified. |
| `start_position` | enum | `{ type: current }` | Where to start on a fresh run (no persisted bookmark). See [Start positions](#start-positions) below. |
| `include_columns` | bool | `true` | Emit the pre-image (`before`) on updates and deletes. Set to `false` to suppress the before-image and reduce payload size. |
| `emit_schema_changes` | bool | `false` | Emit DDL statements (CREATE/ALTER/DROP TABLE etc.) as `{ op: "ddl" }` records. Each DDL auto-commits and advances the bookmark. |
| `tls` | enum | `{ mode: disable }` | TLS for the replication connection. See [TLS](#tls) below. |
| `idle_timeout` | seconds | `30` | Terminate the fetch cycle after this long with no new events. The accumulated page is flushed and the bookmark saved. |
| `max_staged_records` | usize \| null | `null` | Abort if a single in-progress transaction buffers more than this many rows. `null` = unbounded. Set this to avoid OOM on unexpectedly large transactions. |
| `batch_size` | usize | `1000` | Advisory page size. `0` = accumulate all committed transactions into one trailing page (useful for small lookup tables). Per-transaction streaming always emits one page per commit regardless of batch size when the pipeline drives `stream_pages`. |

### Start positions

| Variant | Description |
|---------|-------------|
| `{ type: current }` | Start at the server's current binlog position — skip history. Default. |
| `{ type: earliest }` | Start from the oldest available binlog file. Errors if binlogs have been purged past the earliest available point. |
| `{ type: file_pos, file: "mysql-bin.000003", pos: 4567 }` | Resume from an explicit file/position. |
| `{ type: gtid_set, value: "uuid:1-1000" }` | Start after an executed GTID set. Requires `gtid_mode=ON` on the server. |

### TLS

| Mode | Description |
|------|-------------|
| `{ mode: disable }` | No TLS (default). Credentials and row data travel cleartext. |
| `{ mode: require }` | Require TLS but do not verify the server certificate. |
| `{ mode: verify_ca, ca_path: "/path/to/ca.pem" }` | Require TLS and verify the certificate chain. `ca_path` is optional (uses system roots if omitted). |
| `{ mode: verify_full, ca_path: "/path/to/ca.pem" }` | Require TLS and verify both the chain and the hostname. `ca_path` optional. |

---

## Resumability and state key

The connector is fully resumable. After each committed transaction the pipeline writes the binlog file/position of that commit's end event to the configured `StateStore`. On the next run, `apply_start_bookmark` restores the coordinates and the binlog stream opens from that exact position — MySQL redelivers events from that point with no duplicates and no gap.

**Bookmark strategy:** all persisted bookmarks use `{ file, pos }` (the end-position of the commit event), even when `start_position` is `gtid_set`. This avoids the complexity of accumulating executed-GTID intervals across multiple sessions while still guaranteeing exactly-once resume semantics: `{ file, pos }` is always available, unambiguous, and honoured by the server regardless of whether GTID mode is on.

**Persisted bookmark shape:**

```json
{ "file": "mysql-bin.000003", "pos": 4567 }
```

**State key** (one per `server_id`):

```
mysql-cdc:<server_id>
```

Example: `server_id: 1001` → state key `mysql-cdc:1001`.

---

## Caveats

- **InnoDB / transactional tables are the primary target.** InnoDB commits arrive as `XidEvent`. Explicit `COMMIT` statements from non-transactional engines (MyISAM, CSV, etc.) are also handled as commit boundaries. Mixed-engine transactions are supported but the commit boundary is detected from whichever event arrives first.
- **`binlog_row_metadata=FULL` is required for column names.** Without it, column names are absent from row events. The connector verifies this at startup and fails fast if the variable is not `FULL`. Setting `binlog_row_metadata=MINIMAL` (the default in MySQL 8.0) will cause startup failure.
- **JSON columns emit opaque nested scalars.** MySQL's binlog does not include full JSON diff metadata in ROW format; complex JSON column values may decode as `null` with a one-shot `tracing::warn!`. Use `{ type: update_lookup }` from a separate query source if you need full JSON-column fidelity.
- **`start_position: earliest` may error.** If binlogs have been purged (via `expire_logs_days` or `PURGE BINARY LOGS`), MySQL returns an error when the requested position no longer exists. Keep your binlog retention window large enough for your expected downtime.
- **Per-transaction durability.** The bookmark advances once per committed transaction. A crash mid-transaction replays at most the events since the last persisted bookmark (the incomplete transaction is re-delivered from the last commit boundary).
- **`max_staged_records` guards against OOM.** Very large transactions (bulk INSERTs of millions of rows) buffer all rows in memory before the commit event arrives. Set `max_staged_records` to a safe upper bound for your workload.

---

## Example: capture all changes and write to BigQuery

```yaml
version: 1
source:
  type: mysql-cdc
  config:
    connection_url: mysql://repl:${env:REPL_PASSWORD}@db.prod.internal:3306/myapp
    server_id: 2001
    start_position:
      type: current
    include_tables:
      - myapp.orders
      - myapp.order_items
    include_columns: true
    idle_timeout: 60
    max_staged_records: 100000
    batch_size: 500
sink:
  type: bigquery
  config:
    project_id: my-gcp-project
    dataset_id: cdc
    table_id: mysql_changes
    credentials:
      type: service_account_file
      config:
        path: /secrets/bq-sa.json
state:
  type: redis
  config:
    url: redis://localhost:6379
    namespace: faucet-cdc
```

---

## Example: GTID-based start with TLS

```yaml
version: 1
source:
  type: mysql-cdc
  config:
    connection_url: mysql://repl:secret@db.internal:3306/events
    server_id: 3001
    start_position:
      type: gtid_set
      value: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-23"
    tls:
      mode: verify_full
      ca_path: /etc/ssl/certs/mysql-ca.pem
    idle_timeout: 30
sink:
  type: stdout
  config:
    format: pretty
state:
  type: file
  config:
    path: ./state/mysql_cdc.json
```

---

## See also

- `crates/source/mysql/` — query-mode MySQL source (snapshots via SQL queries).
- `crates/state/redis/` — Redis-backed `StateStore` for durable file/pos bookmarks.
- `crates/state/postgres/` — PostgreSQL-backed `StateStore` alternative.
- `crates/source/postgres-cdc/` — PostgreSQL CDC for comparison (LSN bookmarks, pgoutput).
- `crates/source/mongodb-cdc/` — MongoDB CDC (Change Streams, resumeToken bookmarks).

## Lineage dataset URI

`mysql://<host>:<port>/<db>` or `mysql://<host>:<port>/<db>?tables=<t1>,<t2>` (credentials stripped) — e.g. `mysql://host:3306/app?tables=db.orders,db.users`.

## License

Licensed under MIT or Apache-2.0.
