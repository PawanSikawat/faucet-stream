# faucet-transform-sql

SQL-as-transform for faucet-stream — run DuckDB SQL over each pipeline page. The
page's records are exposed as the relation `batch`; the result set replaces the page.

```toml
[dependencies]
faucet-transform-sql = "0.1"
```

Enable in the umbrella crate or CLI:

```toml
faucet-stream = { version = "1.0", features = ["transform-sql"] }
```

## What it does

Every pipeline page passes through the SQL query as a temporary in-memory DuckDB
table named `batch`. The result set of the query becomes the new page — each result
row becomes one output JSON record. Column name → JSON key; `NULL` → JSON `null`;
`STRUCT`/`LIST`/`MAP` → nested JSON.

This makes it straightforward to filter, reshape, aggregate, and join inline data
without a separate warehouse step.

## Config

```yaml
- type: sql
  config:
    query: "SELECT id, upper(name) AS name FROM batch WHERE active"
```

Wire shape: `{ type: sql, config: { query, relations?, memory_limit?, threads? } }`.

### Field reference

| Field | Type | Required | Description |
|---|---|---|---|
| `query` | string | yes | The SQL statement. `batch` is the page's records as a table. |
| `relations` | list | no | Reference relations loaded at compile time and joinable by name. |
| `memory_limit` | string | no | DuckDB `memory_limit` pragma (e.g. `"1GB"`). Default: DuckDB's own. |
| `threads` | integer | no | DuckDB `threads` pragma. Default: DuckDB's own. Set to 1–2 in high-fan-out matrices to avoid CPU over-subscription. |

## Reference relations

Pre-load static lookup data (CSV, JSONL, or inline values) that your query can `JOIN`
against. Relations are loaded **once at compile time** (when `faucet validate` /
`faucet run` first reads the config) and remain resident for the lifetime of the
transform.

```yaml
- type: sql
  config:
    query: |
      SELECT b.id, c.country
      FROM batch b
      LEFT JOIN countries c ON b.code = c.code
    relations:
      - name: countries
        source:
          type: csv
          path: data/countries.csv
          has_header: true        # default true
```

### Relation source types

| `type` | Fields | Description |
|---|---|---|
| `csv` | `path` (string, required), `has_header` (bool, default `true`) | Loaded via DuckDB `read_csv_auto`. |
| `jsonl` | `path` (string, required) | Loaded via DuckDB `read_json_auto`. |
| `values` | `columns` (list of strings), `rows` (list of lists) | Inline data; no file I/O. |

Inline `values` example:

```yaml
relations:
  - name: tiers
    source:
      type: values
      columns: [id, label]
      rows:
        - [1, gold]
        - [2, silver]
        - [3, bronze]
```

### `reload_on_change`

```yaml
relations:
  - name: prices
    source:
      type: csv
      path: data/prices.csv
    reload_on_change: true   # re-read when the file's mtime changes
```

When `true`, faucet stats the file before each page and rebuilds the relation
atomically if the mtime changed. Defaults to `false`. Ignored for `values`.

### Reserved name

The name `batch` is reserved for the page relation. Using it as a relation name
is a compile-time error.

## Per-page semantics and `batch_size: 0`

**This is the most important thing to understand about the SQL transform.**

The transform runs once **per page**, not once across the whole stream. With the
default `batch_size` of 1000, `GROUP BY` and window functions aggregate within a
single 1000-row page — not across all pages.

```yaml
# BAD: GROUP BY runs per-page, giving partial aggregates.
pipeline:
  source:
    type: csv
    config:
      path: data/orders.csv
  transforms:
    - type: sql
      config:
        query: "SELECT country, SUM(amount) AS total FROM batch GROUP BY country"
```

**To aggregate across the whole dataset**, set the source's `batch_size: 0` so the
entire result set arrives as one page:

```yaml
# CORRECT: batch_size: 0 loads the whole file as one page → global GROUP BY.
pipeline:
  source:
    type: csv
    config:
      path: data/orders.csv
      batch_size: 0
  transforms:
    - type: sql
      config:
        query: "SELECT country, SUM(amount) AS total FROM batch GROUP BY country"
```

`batch_size: 0` means "no batching" — the source emits the entire result set as a
single `StreamPage`. All sources support it; it is appropriate for small lookup
tables and for aggregating transforms like this one.

When an aggregating query receives a second page (i.e. `batch_size` was not set to
`0`), faucet emits a one-time warning:

```
WARN faucet::transform::sql: sql transform with aggregation received multiple pages;
aggregation is per-page — set batch_size: 0 for global aggregation
```

## Error handling and validation

**At config load time** (`faucet validate` / start of `faucet run`):

- The query is parse/bind-checked inside DuckDB. Syntax errors report line and
  column number.
- Reference-relation files that do not exist cause an immediate error (before any
  page is processed).
- A relation named `batch` is rejected.

**At runtime** (per page):

- A query that fails mid-run aborts the pipeline immediately (`FaucetError::Transform`).
- Runtime query errors are **not** routed to the dead-letter queue — they follow the
  same fail-fast policy as every other built-in transform.

Empty result sets are valid: a query that matches zero rows produces zero output
records for that page.

## Runnable example

The file `cli/examples/csv_to_jsonl_sql.yaml` groups order CSV data by country and
joins to a reference countries CSV, writing JSONL output:

```yaml
version: 1
name: csv_to_jsonl_sql

pipeline:
  source:
    type: csv
    config:
      path: cli/examples/data/orders.csv
      has_header: true
      batch_size: 0          # whole file as one page → global GROUP BY

  transforms:
    - type: sql
      config:
        query: |
          SELECT c.country,
                 COUNT(*)                     AS order_count,
                 SUM(CAST(o.amount AS DOUBLE)) AS total_amount
          FROM   batch o
          LEFT JOIN countries c ON o.country_code = c.code
          GROUP BY c.country
          ORDER BY c.country
        relations:
          - name: countries
            source:
              type: csv
              path: cli/examples/data/countries.csv
              has_header: true

  sink:
    type: jsonl
    config:
      path: /tmp/faucet_sql_demo.jsonl
```

Run it (requires the `transform-sql`, `source-csv`, and `sink-jsonl` features):

```bash
faucet run cli/examples/csv_to_jsonl_sql.yaml
```

Output rows look like:

```json
{"country":"Germany","order_count":1,"total_amount":3.0}
{"country":"India","order_count":1,"total_amount":7.0}
{"country":"United States","order_count":2,"total_amount":15.5}
```

## Feature flag

`transform-sql` — not in the default build; included in `full`.

```toml
# Enable only the SQL transform
faucet-stream = { version = "1.0", features = ["transform-sql"] }

# Enable together with specific connectors
faucet-stream = { version = "1.0", features = ["source-csv", "sink-jsonl", "transform-sql"] }

# Enable everything
faucet-stream = { version = "1.0", features = ["full"] }
```

CLI:

```bash
cargo install faucet-cli --features transform-sql
```

## JSON columns

DuckDB's `json_extract` works on string or JSON columns. If a field is a JSON
string, use it directly:

```sql
SELECT json_extract(payload, '$.user.id') AS user_id FROM batch
```

Or cast it first:

```sql
SELECT json_extract(payload::JSON, '$.user.id') AS user_id FROM batch
```

## Timestamp / timezone note

DuckDB's `TIMESTAMP` type is timezone-naive. faucet JSON timestamps are RFC 3339
strings (e.g. `"2026-01-01T12:00:00Z"`). To compare or cast them:

```sql
-- Parse an RFC 3339 string into a DuckDB TIMESTAMP (drops the offset)
SELECT CAST(created_at AS TIMESTAMP) AS ts FROM batch
WHERE CAST(created_at AS TIMESTAMP) > '2026-01-01'::TIMESTAMP

-- Keep the string form, compare lexicographically (safe for UTC-only data)
SELECT * FROM batch WHERE created_at > '2026-01-01T00:00:00Z'
```

If your timestamps include non-UTC offsets, normalise them to UTC with `cast` before
passing to the SQL transform, or parse with `strptime`.

## Library usage

```rust
use faucet_transform_sql::{SqlTransform, SqlTransformConfig};
use faucet_core::stage::{compile_stage, apply_stages_to_page};

let cfg = SqlTransformConfig {
    query: "SELECT id, upper(name) AS name FROM batch".into(),
    relations: vec![],
    memory_limit: None,
    threads: None,
};

let t = SqlTransform::compile(&cfg)?;
let stage = compile_stage(&t.into_page_stage())?;

let output = apply_stages_to_page(records, &[stage])?;
```

## License

Licensed under either of Apache License 2.0 or MIT license at your option.
