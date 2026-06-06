# faucet-source-mongodb-cdc

MongoDB CDC (Change Data Capture) source for [`faucet-stream`](https://github.com/PawanSikawat/faucet-stream). Tails a MongoDB [Change Stream](https://www.mongodb.com/docs/manual/changeStreams/) and emits each per-document change event (insert, update, replace, delete, DDL) as a JSON record. The opaque `resumeToken` is persisted via any `faucet-core` `StateStore`, so pipelines resume exactly where they left off after a restart — no duplicates, no gap.

**Requires a replica set or sharded cluster.** Standalone `mongod` instances do not support Change Streams; the connector fails fast in `new()` with a clear error if a standalone is detected.

---

## Quick start

```yaml
# pipeline.yaml
version: 1
source:
  type: mongodb-cdc
  config:
    connection_uri: mongodb://user:pass@localhost:27017/?replicaSet=rs0
    scope:
      type: collection
      database: appdb
      collection: orders
    full_document: update_lookup
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

## Output record schema

Every change event is one JSON object:

```json
{
  "op": "c",
  "ts_ms": 1779019200000,
  "namespace": { "db": "appdb", "coll": "orders" },
  "document_key": { "_id": "6654a1b2c3d4e5f600000001" },
  "before": null,
  "after": {
    "_id": "6654a1b2c3d4e5f600000001",
    "status": "shipped",
    "total": 49.99
  },
  "update_description": null,
  "resume_token": { "_data": "826654A1B20000000..." }
}
```

### Field reference

| Field | Type | Description |
|-------|------|-------------|
| `op` | string | Operation type: `c` (insert), `u` (update), `r` (replace), `d` (delete), `ddl` (drop / rename / dropDatabase / invalidate) |
| `ts_ms` | number | Wall-clock time of the change in Unix-epoch milliseconds (from `clusterTime`) |
| `namespace` | object \| null | `{ "db": "…", "coll": "…" }`. `null` for cluster-scope events that carry no namespace (e.g. cluster-level invalidate) |
| `document_key` | object \| null | Document identity key (typically `{ "_id": … }`) |
| `before` | object \| null | Pre-image of the document. Populated only when `full_document_before_change` is enabled and the collection has `changeStreamPreAndPostImages` turned on (MongoDB 6.0+). |
| `after` | object \| null | Post-image of the document. Populated on inserts, replaces, and updates when `full_document` is `update_lookup`, `when_available`, or `required`. `null` on deletes. |
| `update_description` | object \| null | Present on `u` events: `{ "updated_fields": {…}, "removed_fields": ["…"], "truncated_arrays": [{…}] }`. `null` for all other op types. |
| `resume_token` | object | Opaque server-assigned token. The pipeline persists this as the page bookmark and passes it to `startAfter` on the next run. |

### op mapping

| MongoDB `operationType` | `op` value |
|------------------------|-----------|
| `insert` | `c` |
| `update` | `u` |
| `replace` | `r` |
| `delete` | `d` |
| `drop`, `rename`, `dropDatabase`, `invalidate` | `ddl` |

---

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_uri` | string | — | MongoDB connection URI. Must point at a replica set (`?replicaSet=rs0`) or sharded cluster. A standalone `mongod` URI causes a hard error at startup. |
| `scope` | enum | `{ type: cluster }` | Change Stream scope: `{ type: cluster }` (all databases), `{ type: database, database: "mydb" }` (one database), or `{ type: collection, database: "mydb", collection: "mycoll" }` (one collection). |
| `operation_types` | string[] | `[]` (all) | Server-side `$match` on `operationType`. Empty = all. Example: `["insert", "update", "delete"]`. |
| `full_document` | enum | `off` | Post-image delivery: `off` (no full doc), `when_available` (include when available), `required` (fail if unavailable), `update_lookup` (re-read from server at event time — at-least-once / read-skew caveat, see below). |
| `full_document_before_change` | enum | `off` | Pre-image delivery: `off`, `when_available`, or `required`. Requires MongoDB 6.0+ and `changeStreamPreAndPostImages` enabled on the collection. |
| `start_from` | enum | `{ type: now }` | Where to start if no persisted bookmark exists: `{ type: now }` (new events only), `{ type: earliest }` (from the oldest available oplog entry — errors if the oplog has rolled past), `{ type: resume_token, token: {…} }` (explicit token, always used regardless of any persisted bookmark), or `{ type: timestamp, timestamp_secs: N }` (cluster time in seconds, always used regardless of any persisted bookmark). |
| `aggregation_pipeline` | object[] | `[]` | Extra aggregation stages appended **after** the internal `$match` on `operation_types`. Use for server-side field projection, filtering, or transformation. |
| `idle_timeout` | seconds | `30` | Terminate the current fetch cycle after this long with no new events. The page accumulated so far is flushed and the bookmark is saved. |
| `max_await_time_ms` | u64 | `1000` | Server-side `maxAwaitTimeMS` on `getMore` requests. Controls how long the server blocks before returning an empty batch. Must be less than `idle_timeout` (in milliseconds). |
| `batch_size` | usize | `1000` | Records per emitted `StreamPage`. `0` = no batching — drain until idle and emit one page. |

---

## Resumability and state key

The connector is fully resumable. After each emitted page the pipeline writes the `resume_token` of the last event in that page to the configured `StateStore` (per-batch durability). On the next run, `apply_start_bookmark` restores the token and the Change Stream opens with `startAfter: <token>` — MongoDB delivers events from exactly that point onwards with no duplicates and no gap.

**`start_from` precedence:**

- `resume_token` and `timestamp` variants **always** override a persisted bookmark — they force an explicit start position regardless of what the state store holds.
- `now` and `earliest` variants **yield** to a persisted bookmark: if one exists the connector resumes from it; if none exists the specified variant is used as the initial position.

State keys used (one per scope):

| Scope | State key |
|-------|-----------|
| Cluster | `mongodb-cdc:cluster` |
| Database `mydb` | `mongodb-cdc:db:mydb` |
| Collection `mydb.mycoll` | `mongodb-cdc:coll:mydb.mycoll` |

---

## invalidate events

An `invalidate` event (emitted when a watched collection or database is dropped while the stream is open) is delivered as a `ddl` record and then **terminates the stream**. The connector closes the stream and returns; a fresh `faucet run` is needed to start a new Change Stream. Persisted bookmarks from before the invalidate are discarded by MongoDB — use `start_from: { type: now }` or `{ type: earliest }` on the next run.

---

## Caveats

- **Replica set or sharded cluster required.** Standalone `mongod` instances do not support Change Streams. The connector validates this at startup and returns a typed `FaucetError::Config` immediately.
- **`full_document: update_lookup` has at-least-once / read-skew semantics.** The document is re-read from the primary at event delivery time, not at the time of the original change. If the document has been further modified or deleted between the change and the lookup, the `after` image will reflect the later state (or be absent). Do not rely on `after` for exactly-once semantics when using `update_lookup`.
- **`start_from: earliest` may error.** If the oplog has rolled past the earliest available timestamp (common on busy clusters with a short oplog window), MongoDB returns an error. Use `{ type: now }` for new deployments and keep your oplog window large enough for your expected downtime.
- **`full_document_before_change` requires MongoDB 6.0+ and per-collection configuration.** Enable pre-images on the collection before starting the stream:
  ```js
  db.runCommand({
    collMod: "mycoll",
    changeStreamPreAndPostImages: { enabled: true }
  });
  ```
  Without this, `required` will error; `when_available` will produce `before: null`.
- **DDL events are best-effort.** Schema changes (collection drops, renames) arrive as `ddl` records, but the Change Stream does not replicate index operations or user-visible `collMod` changes that don't appear in the oplog.
- **Per-batch durability, not per-event.** The bookmark is saved once per emitted page (every `batch_size` events), not after every individual event. A crash mid-page replays at most `batch_size` events.

---

## Example: capture all changes to a collection

```yaml
version: 1
source:
  type: mongodb-cdc
  config:
    connection_uri: mongodb://faucet:secret@mongo1:27017,mongo2:27017/?replicaSet=rs0&authSource=admin
    scope:
      type: collection
      database: myapp
      collection: events
    operation_types: ["insert", "update", "replace", "delete"]
    full_document: when_available
    full_document_before_change: off
    idle_timeout: 60
    max_await_time_ms: 500
    batch_size: 500
sink:
  type: bigquery
  config:
    project_id: my-gcp-project
    dataset_id: cdc
    table_id: events_stream
    credentials:
      type: service_account_file
      config: { path: /secrets/bq-sa.json }
state:
  type: redis
  config:
    url: redis://localhost:6379
    namespace: faucet-cdc
```

---

## Example: cluster-wide CDC with operation filtering

```yaml
version: 1
source:
  type: mongodb-cdc
  config:
    connection_uri: mongodb://faucet:secret@mongos:27017/?tls=true
    scope:
      type: cluster
    operation_types: ["insert", "delete"]
    idle_timeout: 30
sink:
  type: stdout
  config:
    format: pretty
state:
  type: file
  config: { path: ./state/mongodb-cdc }
```

---

## See also

- `crates/source/mongodb/` — query-mode MongoDB source (snapshots via `find()`).
- `crates/state/redis/` — Redis-backed `StateStore` for durable resumeToken bookmarks.
- `crates/state/postgres/` — PostgreSQL-backed `StateStore` alternative.
- `crates/source/postgres-cdc/` — PostgreSQL CDC for comparison (LSN bookmarks, pgoutput).
