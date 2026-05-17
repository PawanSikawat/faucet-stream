# faucet-source-postgres-cdc

PostgreSQL CDC (Change Data Capture) source for [`faucet-stream`](https://github.com/PawanSikawat/faucet-stream). Subscribes to a Postgres logical replication slot using the `pgoutput` plugin and emits each row-level change (INSERT / UPDATE / DELETE / TRUNCATE) as a JSON record. Replication position is persisted via any `faucet-core` `StateStore`, so pipelines resume exactly where they left off — no duplicates, no gap.

---

## Quick start

```yaml
# pipeline.yaml
version: 1
source:
  type: postgres-cdc
  config:
    connection_url: postgres://user:pass@localhost:5432/appdb
    slot_name: faucet_slot
    publication_name: faucet_pub
    create_slot_if_missing: true
    idle_timeout: 30
sink:
  type: jsonl
  config:
    path: ./changes.jsonl
state:
  type: file
  config: { path: ./state }
```

```bash
faucet run pipeline.yaml
```

---

## Postgres setup (one-time)

```sql
-- 1. Database needs logical replication enabled.
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;
-- restart Postgres after this

-- 2. The role that connects must have REPLICATION.
ALTER ROLE faucet_user WITH REPLICATION;

-- 3. Create a publication for the tables you want to capture.
CREATE PUBLICATION faucet_pub FOR TABLE public.users, public.orders;
-- or FOR ALL TABLES if you really want everything.

-- 4. (Optional but recommended) get a full pre-image on UPDATE/DELETE.
ALTER TABLE public.users REPLICA IDENTITY FULL;
```

The replication slot is created automatically on first run when
`create_slot_if_missing = true` (the default).

---

## Output record schema

Every change event is one JSON object:

```json
{
  "op":     "insert | update | delete | truncate",
  "schema": "public",
  "table":  "users",
  "lsn":    "0/16A4F88",
  "ts_ms":  1779019200000,
  "before": null | { ...row... },
  "after":  null | { ...row..., "__unchanged_toast__": ["col1", "col2"] }
}
```

- `lsn` is the `commit_lsn` of the enclosing transaction.
- `ts_ms` is Unix-epoch milliseconds, derived from the COMMIT's timestamp.
- `before` is populated on `delete` always, and on `update` only when the
  table is `REPLICA IDENTITY FULL`. Otherwise it is `null`.
- `after` is populated on `insert` and `update`, and `null` on `delete` /
  `truncate`.
- If any column arrived as "unchanged TOAST" (Postgres elides large
  out-of-line values whose stored copy was not rewritten), the column is
  dropped from `before` / `after` and its name is recorded in
  `before.__unchanged_toast__` / `after.__unchanged_toast__`.

`truncate` emits one record per truncated relation with `before = after = null`.

---

## Configuration

| Field                       | Type     | Default | Description |
|-----------------------------|----------|---------|-------------|
| `connection_url`            | string   | —       | Postgres connection URL. |
| `slot_name`                 | string   | —       | Logical replication slot. Must match `[a-z0-9_]{1,63}`. |
| `publication_name`          | string   | —       | Publication that selects which tables are replicated. |
| `create_slot_if_missing`    | bool     | `true`  | Create the slot on first run if it does not exist. |
| `start_lsn`                 | string?  | `null`  | One-time override (e.g. `"0/16A4F88"`); ignored if a state-store bookmark exists. |
| `proto_version`             | u32      | `1`     | pgoutput protocol version. Only `1` is supported in this release. |
| `idle_timeout`              | seconds  | `30`    | Stop the current fetch cycle after this long without a new replication message. |
| `max_messages`              | usize?   | `null`  | Optional cap on change events per fetch call. The cap is checked **after each COMMIT**; a transaction larger than `max_messages` still emits atomically. |
| `status_update_interval`    | seconds  | `10`    | Standby Status Update cadence. Must be `< idle_timeout` and well under the server's `wal_sender_timeout`. |
| `tcp_keepalive`             | seconds  | `60`    | TCP keepalive on the replication connection. |

---

## Transactional consistency

The connector buffers each transaction in memory and only flushes its records
to the sink on `COMMIT`. Partial transactions (BEGIN seen, no COMMIT yet
within `idle_timeout` / `max_messages`) are dropped and redelivered after the
next `START_REPLICATION`. This makes the output transactionally consistent —
sinks never see half a transaction — at the cost of needing each
transaction to fit within one fetch cycle. Size `max_messages` accordingly
or leave it unset for unbounded buffering.

---

## At-least-once semantics

- Postgres redelivers everything after the most recent `confirmed_flush_lsn`
  on every `START_REPLICATION`, with no duplicates and no gap.
- `faucet-stream`'s pipeline only writes the new bookmark to the state store
  **after the sink confirms** the batch was flushed.
- On the next run, `apply_start_bookmark` is called with that bookmark and
  the connector advances `confirmed_flush_lsn` accordingly.
- If the process crashes between sink flush and state-store write, the next
  run will replay the most recent transaction. Sinks should be idempotent or
  accept duplicates at transaction boundaries.

---

## Operational caveats

- **Slot bloat:** without a state store the connector never advances
  `confirmed_flush_lsn`, so Postgres retains WAL indefinitely. Always
  configure a state store in production.
- **Heartbeats:** if the network is silent for longer than the server's
  `wal_sender_timeout` (default 60 s), Postgres kills the replication
  connection. The default `status_update_interval` of 10 s leaves ample
  margin.
- **DDL is invisible.** Logical replication does not replicate schema
  changes. After an `ALTER TABLE`, the next change event from that table
  will arrive with a new `Relation` message — the connector picks it up
  automatically.
- **Reconnection on drop:** v1 surfaces a connection drop as
  `FaucetError::Source`. Re-run the pipeline to reconnect; the persisted
  LSN bookmark ensures no data loss.
- **Slot creation timing:** the slot is created on the first fetch call.
  Changes applied **before** the slot exists are not replicated. To avoid
  losing initial changes, either create the slot before applying writes
  (e.g. via `psql`) or do a warm-up fetch and then begin writes.

---

## Implementation notes

Built on top of the [`pgwire-replication`](https://crates.io/crates/pgwire-replication)
crate for the Postgres logical-replication wire protocol; the `pgoutput`
payload bytes are decoded by a hand-rolled decoder in this crate so that the
output record shape stays under our control.

---

## See also

- `crates/source/postgres/` — query-mode Postgres source (snapshots, not CDC).
- `crates/state/postgres/` — Postgres-backed `StateStore` you can pair with this source.
- `cli/examples/postgres_cdc_to_jsonl.yaml` — end-to-end demo configuration.
