# Record transforms

A pipeline's `transforms:` list is a sequence of pure `Fn(Value) -> Value`
steps run on every record between source and sink. Each transform is a
small, declarative reshape — pick the ones you need, list them in the
order you want them to run, and the CLI wires them up for you.

This page is a tour of the standard transforms exposed in YAML. All of
them are listed in `faucet list` and dispatchable as `type:` values.

## At a glance

| Kind | Purpose | Shape |
|---|---|---|
| `flatten` | Collapse nested objects to a flat record | `separator` |
| `rename_keys` | Regex rename of every key, recursively | `pattern`, `replacement` |
| `keys_case` | Re-case every key (snake / camel / pascal / kebab / screaming_snake / dot) | `mode` |
| `spell_symbols` | Spell out symbols in keys (`%` → `percent`, `#` → `number`, …) | `extra`, `separator` |
| `select` | Keep only listed top-level fields | `fields: [..]` |
| `drop` | Remove listed top-level fields | `fields: [..]` |
| `set` | Add or overwrite top-level fields with constants | `values: {k: v, ..}` |
| `rename_field` | Exact-name rename (vs. regex) | `fields: {from: to, ..}` |
| `cast` | Coerce per-field types | `fields: {name: type}`, `on_error` |
| `redact` | Replace listed field values with a mask | `fields: [..]`, `mask` |
| `hash` | Hash fields (SHA-256 / BLAKE3) into stable, join-able tokens | `fields: [..]`, `algorithm`, `encoding`, `salt?`, `into?` |
| `json_parse` | Parse a stringified-JSON field into a nested value | `fields: [..]`, `on_error`, `into?` |
| `coalesce` | Fill a missing/null field from a default or first non-null key | `field`, `default` \| `from: [..]`, `treat_empty_string_as_null` |
| `value_case` | Lowercase / uppercase / trim / title / capitalize string values | `fields: [..]`, `mode` |
| `split` | Split a string field into an array on a delimiter | `field`, `delimiter`, `trim`, `into?` |
| `join` | Join an array field into a string with a delimiter | `field`, `delimiter`, `into?` |
| `json_encode` | Serialize a nested field to a JSON string (inverse of `json_parse`) | `fields: [..]` |
| `unpivot` | Reshape wide columns or a map field into long key/value rows (1→N) | `id_fields`, `key_name`, `value_name`, `columns?` \| `from?`, `drop_nulls?` |
| `lookup` | Enrich records by joining an inline / JSONL reference table | `values` \| `jsonl`, `on: {record, ref}`, `add: {out: ref_col}`, `on_missing?` |
| `sql` | Run DuckDB SQL over the whole page; records are the `batch` relation | `query`, `relations?`, `memory_limit?`, `threads?` · page-level (sees the whole batch) · needs `transform-sql` feature · [cookbook](./sql-transform.md) |
| `wasm` | Run a user-provided sandboxed `.wasm` module over each record | `module`, `function?`, `memory_limit_mb?`, `fuel_limit?`, `on_error?`, `reload_on_change?` · per-record · needs `transform-wasm` feature · [cookbook](./wasm-transforms.md) |

The field-targeting transforms (`select`, `drop`, `set`, `rename_field`,
`cast`, `redact`, `value_case`) act on **top-level** fields only —
dotted paths into nested objects are intentionally out of scope. If you
need to reach a nested field, run `flatten` first, then operate on the
flattened key.

Missing fields are silently skipped. None of the field-selection
transforms introduce a `null` for a name that wasn't already on the
record.

## A full example

The runnable file is at `cli/examples/rest_to_stdout_transforms.yaml`:

```yaml
pipeline:
  source:
    type: rest
    config: { ... }

  transforms:
    - type: flatten
      config: { separator: "__" }
    - type: select
      config:
        fields: [id, name, email, address__city, company__name]
    - type: rename_field
      config:
        fields:
          address__city: city
          company__name: company
    - type: value_case
      config:
        fields: [email]
        mode: lower
    - type: cast
      config:
        fields: { id: string }
        on_error: error
    - type: redact
      config:
        fields: [phone]
        mask: "[redacted]"
    - type: set
      config:
        values:
          _source: jsonplaceholder
          _ingested_at: "2026-01-01T00:00:00Z"

  sink:
    type: stdout
    config: { format: json_lines }
```

Run it:

```bash
faucet run cli/examples/rest_to_stdout_transforms.yaml | jq .
```

The order matters: `flatten` runs first so that `select` can reference
`address__city`; `rename_field` runs after `select` so it only has to
rename keys that survived; `cast` runs before `set` so the stamped
`_source` field is left untouched.

## Declaration layers

Transforms can be declared at three layers in a config. The executor
resolves them per matrix row by concatenating contributions in
*lifecycle order* — pipeline first, then source template, then row:

```
final = T_pipeline ++ T_source ++ T_row
```

| Layer | Lives at | Intent |
|---|---|---|
| Pipeline | `pipeline.transforms` | cross-cutting policy (PII redaction, provenance stamp) |
| Source template | `pipeline.sources.<name>.transforms` | cleanup tied to the source's natural emission shape |
| Matrix row | `matrix[i].transforms` | row-specific extras or one-off shaping |

Each layer is optional. Empty layers contribute nothing.

```yaml
pipeline:
  transforms:                                  # T_pipeline (runs first)
    - { type: set, config: { values: { _ingested_at: "${env:NOW}" } } }
  sources:
    users_api:
      type: rest
      transforms:                              # T_source
        - { type: flatten, config: { separator: "__" } }
        - { type: keys_case, config: { mode: snake } }
matrix:
  - id: users_pii
    source: { ref: users_api }
    transforms:                                # T_row (runs last)
      - { type: redact, config: { fields: [email], mask: "[pii]" } }
    # final = [set, flatten, keys_case, redact]
```

## Opting out: `inherit_transforms: false`

Each layer that *introduces* transforms (source template, matrix row) carries
a sibling boolean field `inherit_transforms`, default `true`. Set to `false`,
it drops every layer declared above it.

| `source.inherit_transforms` | `row.inherit_transforms` | Final list |
|---|---|---|
| `true` (default) | `true` (default) | `T_pipeline ++ T_source ++ T_row` |
| `false` | `true` | `T_source ++ T_row` |
| `true` | `false` | `T_row` |
| `false` | `false` | `T_row` |

Use this for debug rows that need raw records, or for a source whose natural
shape is already canonical and shouldn't be touched by global policy:

```yaml
matrix:
  - id: forensic_row
    source: { ref: users_api }
    inherit_transforms: false              # ← drops T_pipeline AND T_source
    transforms:
      - { type: select, config: { fields: [id, raw_payload] } }
    # final = [select]
```

Sinks reject both `transforms:` and `inherit_transforms:`. Destination shaping
belongs at the pipeline or row layer.

## Reusing transform lists across sources

Use YAML anchors:

```yaml
pipeline:
  sources:
    users_api:
      type: rest
      transforms: &user_cleanup
        - { type: flatten, config: { separator: "__" } }
        - { type: keys_case, config: { mode: snake } }
    archived_users_api:
      type: rest
      transforms: *user_cleanup
```

No grammar extension needed — the YAML parser expands anchors before the
config reaches `faucet`.

## `keys_case` — pick the output convention

```yaml
- type: keys_case
  config:
    mode: snake   # | camel | pascal | kebab | screaming_snake | dot
```

The tokeniser splits each key on whitespace, `_`, `-`, dropped
punctuation, and lower→upper transitions (so `"firstName"` and
`"first_name"` and `"first-name"` all tokenise the same), then re-joins
in the requested style:

| Input          | `snake`        | `camel`       | `pascal`     | `kebab`        | `screaming_snake` | `dot`          |
|----------------|----------------|---------------|--------------|----------------|-------------------|----------------|
| `"First Name"` | `first_name`   | `firstName`   | `FirstName`  | `first-name`   | `FIRST_NAME`      | `first.name`   |
| `"last-name"`  | `last_name`    | `lastName`    | `LastName`   | `last-name`    | `LAST_NAME`       | `last.name`    |
| `"camelCase"`  | `camel_case`   | `camelCase`   | `CamelCase`  | `camel-case`   | `CAMEL_CASE`      | `camel.case`   |
| `"ID"`         | `id`           | `id`          | `Id`         | `id`           | `ID`              | `id`           |

`dot` (`dot.case`) is handy for backends that expect dotted field names
(some search / metrics systems). It tokenises identically to the other
modes — only the join separator differs.

Two distinct keys that re-case to the same name error rather than
silently overwriting (same collision rule as `flatten` and
`spell_symbols`). An all-symbol key (`"!@#"`) tokenises to nothing and
is kept as-is to avoid producing a blank key.

Multi-char uppercase runs are left as one token: `"XMLParser"` →
`["XMLParser"]` → `xmlparser` (snake). If you need them split, normalise
with `rename_keys` first.

## `spell_symbols` — symbols → words in keys

```yaml
- type: spell_symbols
  config:
    extra:
      "©": copyright
      "<=": lte
    separator: " "   # default
```

The default map covers the common ASCII symbols:

| `%` → `percent` | `#` → `number` | `$` → `dollar` | `&` → `and` | `@` → `at` |
| `+` → `plus` | `*` → `star` | `=` → `equals` | `<` → `lt` | `>` → `gt` |
| `/` → `slash` | `\` → `backslash` | `|` → `pipe` | `^` → `caret` | `~` → `tilde` |

User entries in `extra` are merged on top of the defaults (an override
with the same key wins). Replacements are sorted longest-first, so
`"<="` beats `"<"` when both are present.

Each replacement is surrounded by `separator` (default `" "`) so a
chained `keys_case` cleanly picks up the word boundary:

```yaml
transforms:
  - type: spell_symbols
  - type: keys_case
    config: { mode: snake }
```

turns `"% sold"` → `" percent sold"` → `"percent_sold"`.

## `select` vs. `drop`

```yaml
- type: select
  config:
    fields: [id, email]
```

Listed fields are kept; everything else is dropped.

```yaml
- type: drop
  config:
    fields: [password, ssn]
```

Listed fields are removed; everything else is kept. Use `select` when
the schema is fixed and you want to defend against the source adding
new fields you don't want; use `drop` for targeted PII / secret
removal.

## `set` — constant stamps

```yaml
- type: set
  config:
    values:
      _source: my-api
      _ingested_at: "2026-05-28T00:00:00Z"
      version: 2
      tags: [pii-free]
```

Any JSON value is accepted (string, number, bool, null, array, object).
Existing fields with the same name are **overwritten** — `set` is the
intentional "I want this value" transform.

## `rename_field` vs. `rename_keys`

Both transforms rename keys, but they're aimed at different jobs:

| `rename_keys` | `rename_field` |
|---|---|
| Single regex substitution applied to every key, recursively (including keys inside nested objects and arrays). | Exact-name match on top-level keys only. |
| Best for systematic patterns: `^_sdc_` → `""`, `([a-z])([A-Z])` → `$1_$2`. | Best for a handful of explicit renames: `address__city` → `city`. |

`rename_field` errors if a target name already exists on the record
(same collision rule as `flatten` and `keys_case`) — to avoid silently
overwriting a real value.

## `cast` — type coercion

```yaml
- type: cast
  config:
    fields:
      age: int
      price: float
      active: bool
      id: string
      created_at: timestamp
    on_error: error
```

Target types: `int` (i64), `float` (f64), `bool`, `string`, `timestamp`
(RFC 3339). `bool` from a string accepts `true|false|1|0|yes|no`
case-insensitively. `timestamp` parses RFC 3339 / ISO 8601 and
normalises the output (so `+00:00` becomes `Z`). Casting a **float to
`int`** only succeeds for a whole number within i64 range — a fractional
value (e.g. `3.9`) or one beyond ±9.2e18 is treated as uncastable (governed
by `on_error`) rather than being silently truncated or saturated.

Failure behaviour is controlled by `on_error`:

| `on_error` | What happens on an uncastable value |
|---|---|
| `error` *(default)* | The transform errors with `FaucetError::Transform`. The pipeline either aborts or routes the record to the DLQ, depending on your DLQ config. |
| `null` | The value is replaced with `null`. Use when the schema must hold and a downstream nullable column is acceptable. |
| `skip` | The value is left as-is (original type). Use when downstream code already handles mixed types. |

Missing fields are always a no-op — `cast` will never insert a `null` for
a field that wasn't already on the record.

Casting epoch seconds / millis to a timestamp is out of scope for the
initial release; file a follow-up issue if you need it.

## `redact`

```yaml
- type: redact
  config:
    fields: [password, ssn, credit_card]
    mask: "***"
```

`mask` is any JSON value (default `"***"` if omitted). Missing fields
are skipped — `redact` will not add `"***"` to a record that didn't
have the field.

> For a policy-driven layer that *detects* PII by value (whatever the column is
> called), reaches into nested paths, hashes/tokenizes for joinable pseudonyms,
> and scopes rules per destination sink, see
> [PII detection & masking](./masking.md).

## `hash`

```yaml
- type: hash
  config:
    fields: [email, user_id]   # one or more fields
    algorithm: sha256          # sha256 (default) | blake3
    encoding: hex              # hex (default) | base64
    salt: "${env:HASH_SALT}"   # optional; prepended before hashing
    into: null                 # optional target key (single field only); null = in place
```

Unlike `redact` (which destroys the value), `hash` produces a **stable,
join-able token**: the same input always maps to the same digest, so
downstream joins still work while the raw PII never reaches a sink.
String values are hashed over their raw UTF-8 bytes; every other JSON
value is hashed over its canonical serialization. Missing fields are
skipped. `into` is only valid with exactly one field (a config error
otherwise); with multiple fields each is replaced in place. Needs the
`transform-hash` feature.

> This is pseudonymization, not a secret — an unsalted digest is
> recomputable by anyone. For keyed, policy-driven hashing see
> [PII detection & masking](./masking.md).

## `json_parse`

```yaml
- type: json_parse
  config:
    fields: [payload, metadata]   # dotted keys holding JSON strings
    on_error: keep                # keep (default) | null | error
    into: null                    # optional target key (single field only); null = in place
```

Expands a stringified-JSON column into a real nested value the rest of
the pipeline (and the sink) can see — pairs naturally with `flatten`
(parse, then flatten). Values that are already objects/arrays (or any
non-string) pass through unchanged (idempotent); missing fields are
skipped. Parse failures follow `on_error`: `keep` leaves the string,
`null` replaces it with `null`, `error` aborts (or routes to the DLQ). A
1→1 transform can't drop a record, so there is no `skip_record` — chain
a `filter` if you need to drop rows whose JSON failed. Needs the
`transform-json-parse` feature.

## `coalesce`

```yaml
- type: coalesce
  config:
    field: status
    # exactly one of:
    default: "unknown"            # a literal JSON value, OR
    from: [status, state]         # first non-null among these keys wins
    treat_empty_string_as_null: false
```

Fills a **missing or null** field — the "set only if absent" primitive
that `set` (which always overwrites) can't express. Exactly one of
`default` / `from` must be set (a config error otherwise). A present,
non-null target is left unchanged (idempotent). With
`treat_empty_string_as_null: true`, an empty string counts as null for
both the target and the `from` keys. If every `from` key is null/absent
and no `default` is given, the target is left as-is. Needs the
`transform-coalesce` feature.

## `value_case`

```yaml
- type: value_case
  config:
    fields: [email, username]
    mode: lower   # | upper | trim | title | capitalize
```

Only string field values are touched; non-string values (numbers, bools,
nulls, nested objects) pass through unchanged.

- `title` upper-cases the first letter of each **whitespace-delimited**
  word and lower-cases the rest (`"new york"` → `"New York"`);
  punctuation and underscores do not start a new word.
- `capitalize` upper-cases only the first character of the whole string
  and lower-cases the rest (`"hELLO wORLD"` → `"Hello world"`).

Both use `char::to_uppercase` semantics (no locale-aware casing).

## `split` / `join`

```yaml
- type: split
  config: { field: tags, delimiter: ",", trim: true, into: null }
- type: join
  config: { field: tags, delimiter: ",", into: null }
```

`split` turns a delimited string into an array; `join` is its inverse.
Both are no-ops on the wrong type (`split` on a non-string, `join` on a
non-array) or a missing field. With `trim`, `split` whitespace-trims each
element but **keeps empty segments**. `join` renders non-string elements
via their JSON scalar form (strings raw, `null` as empty, everything else
as compact JSON). An empty `delimiter` on `split` yields a single-element
array holding the whole string (rather than splitting between every
char). When `into` is set the result is written there, else in place.
Needs the `transform-split-join` feature.

## `json_encode` — nested field → JSON string

```yaml
- type: json_encode
  config: { fields: [address, line_items] }
```

The inverse of `json_parse`: each named field whose value is an object or
array is replaced **in place** with its compact JSON-string form — the
standard step for landing nested data as a flat `STRING` column (e.g. when
matching a warehouse table that stores nested structures as text). Scalar
(already-flat) values and absent fields are left unchanged (idempotent).
Needs the `transform-json-encode` feature.

## `unpivot` — wide/map → long (1→N)

```yaml
# Wide form: monthly columns → one row per month.
- type: unpivot
  config:
    id_fields: [account_id]        # copied onto every output row
    key_name: month                # column name → this field
    value_name: amount             # cell value → this field
    # columns: [jan, feb, mar]     # optional; default = all non-id fields
    drop_nulls: true               # skip null cells

# Map form: expand an object field's entries into rows.
- type: unpivot
  config:
    id_fields: [report_id]
    from: cells                    # the object field to expand
    key_name: column
    value_name: value
```

`unpivot` reshapes each record into **N rows** — one per selected column
(wide form) or per entry of the `from` object (map form) — carrying
`id_fields` onto each. Output rows contain only `id_fields` plus the
key/value pair. When the reshape yields nothing (missing `from`, or no
columns) the original record is **passed through unchanged** unless
`drop_if_empty: true` — records are never silently dropped. This replaces
the SQL you'd otherwise write for gross-to-net / period-report / timeseries
data. Needs the `transform-unpivot` feature.

## `lookup` — enrich from a reference table (no SQL)

```yaml
- type: lookup
  config:
    values:                                  # inline reference rows …
      - { id: "1", name: "North America" }
      - { id: "2", name: "EMEA" }
    # jsonl: ./ref/regions.jsonl             # … or a JSONL file (one object/line)
    on: { record: region_id, ref: id }       # match record.region_id == ref.id
    add: { region_name: name }               # add record.region_name = ref.name
    on_missing: null                         # null (default) | keep | error
```

`lookup` joins each record against a small in-memory reference set by key
(compared by scalar-string form, so `42` matches `"42"`) and writes the
`add` columns onto the record — a code→label enrichment without a SQL
transform. It is 1→1 (never drops rows): on a miss it writes the added
columns as `null` (`null`), leaves the record untouched (`keep`), or fails
the batch (`error`). The reference is resolved once at config-load. Needs
the `transform-lookup` feature.

## Ordering rules of thumb

Transforms run **in the order you list them**, so think about
dependencies:

- `flatten`, `spell_symbols`, and `keys_case` change key names — list
  field-targeting transforms (`select`, `drop`, `cast`, `redact`,
  `value_case`, `rename_field`) **after** them, referencing the
  post-rename keys.
- `cast` runs before downstream consumers see the record, so put it
  after any rename steps but before `set` if you want `set`'s stamped
  values left untouched.
- `set` overwrites by name — put it last when you want it to win.

The "clean keys for a downstream warehouse" pipeline is canonical:

```yaml
transforms:
  - type: spell_symbols     # %sold → percent sold
  - type: keys_case
    config: { mode: snake } # percent sold → percent_sold
  - type: rename_field
    config:
      fields: { legacy_id: id }
```

## Out of scope

- **Dotted-path field selection on the field-list transforms** (`select`,
  `drop`, `cast`, `redact`, `value_case`, `rename_field`) — they still
  operate on bare top-level keys. Run `flatten` first if you need nested
  access. `filter` and `explode` are the exceptions and support the
  JSONPath subset documented in their sections.
- **A general expression / scripting transform (jq, CEL, …)** —
  separate, larger discussion.

## Filter and explode

### Filter — keep records matching a predicate

```yaml
transforms:
  - { type: filter, config: { path: deleted, op: ne, value: true } }
```

Operators: `eq`, `ne`, `exists`, `in`, `not_in`.

- `path:` — JSONPath subset: bare key (`status`), dot path (`$.user.status`), or bracketed string key (`$['order-id']`). Bare keys are auto-prefixed with `$.`. Keys that literally contain `.` require the `$`-rooted bracket form (`"$['foo.bar']"`).
- `value:` — required for `eq` / `ne` / `in` / `not_in`. For `in` / `not_in`, must be an array. Forbidden for `exists`.
- Type semantics: strict JSON equality. `"5" eq 5` is false. Chain `cast` upstream to coerce.
- `ne` and `not_in` **keep records with a missing path** (the predicate is satisfied by absence). All other operators drop missing-path records.

### Explode — expand an array into one record per element

```yaml
transforms:
  - { type: explode, config: { path: items, prefix: item } }
```

- `path:` — same JSONPath subset as filter.
- `prefix:` — prepended to each element field when the element is an object. Defaults to the last segment of `path` (so `path: items` ⇒ `prefix: items`). Empty string opts out of prefixing (pure LATERAL FLATTEN).
- `separator:` — between prefix and element field key. Default `"_"`.
- `on_missing:` — what to do when the path doesn't yield a non-empty array. `passthrough` (default — record flows through unchanged), `drop` (SQL `UNNEST` semantics), or `error`.

**Merge rule (object elements):** the array node at `path` is removed from its parent container and each element field is added as a sibling, prefixed.

| Input | Stage | Output |
|---|---|---|
| `{id: 1, items: [{sku: A, qty: 2}]}` | `explode { path: items }` | `{id: 1, items_sku: A, items_qty: 2}` |
| `{id: 1, items: [{sku: A}, {sku: B}]}` | `explode { path: items, prefix: item }` | `{id: 1, item_sku: A}`, `{id: 1, item_sku: B}` |
| `{id: 1, items: [{sku: A}], prefix: ""}` | `explode { path: items, prefix: "" }` | `{id: 1, sku: A}` |
| `{id: 1, tags: ["rust", "etl"]}` | `explode { path: tags }` | `{id: 1, tags: rust}`, `{id: 1, tags: etl}` |
| `{id: 1, user: {name: A, items: [{x: 1}]}}` | `explode { path: $.user.items }` | `{id: 1, user: {name: A, items_x: 1}}` |

**Collisions** (a prefixed element key would overwrite a sibling) fail loudly with `FaucetError::Transform("explode produced duplicate key 'X'")` — mirroring `flatten` / `keys_case`.

### Ordering: explode early, filter late (usually)

The recommended order is `explode → transform → filter`: each child of the explode gets transforms applied uniformly, and the final filter acts on cleaned shape. Two legitimate deviations:

- **filter before explode**: drop soft-deleted parents *before* exploding, saving the work of expanding children of dead rows.
- **filter both sides**: drop dead parents, explode, then drop archived children.

```yaml
transforms:
  - { type: filter, config: { path: deleted, op: ne, value: true } }
  - { type: explode, config: { path: items, prefix: item } }
  - { type: filter, config: { path: item_status, op: in, value: [active, pending] } }
  - { type: keys_case, config: { mode: snake } }
```

## `cdc_unwrap` — normalize CDC change events into flat rows

The CDC sources (`postgres-cdc`, `mysql-cdc`, `mongodb-cdc`) emit change-event
**envelopes** — a wrapper carrying an operation code and the row's before/after
images — not the bare rows themselves. `cdc_unwrap` flattens that envelope into a
single row plus an `__op` marker, so a downstream
[upsert sink](./upsert.md) can mirror the change without understanding CDC at all.
It's the standard first transform in a CDC → mirror pipeline:

```yaml
transforms:
  - type: cdc_unwrap
```

For each change event it:

- **drops** DDL / truncate events (`op` ∈ `drop_ops`) — they have no row to mirror;
- for a **delete** (`op` ∈ `delete_ops`), emits the pre-image (`before`), falling
  back to `key_field` (MongoDB carries the key in `document_key` when there is no
  `before`); rows with no usable key are dropped with a `tracing::warn!`;
- for an **insert / update**, emits the post-image (`after`); events with no row
  image are dropped with a warning;
- stamps every emitted row with a `marker_field` (`__op`) set to the normalized
  value **`"d"`** (delete) or **`"u"`** (upsert) — *not* the raw op code. A
  downstream sink's `delete_marker` should therefore match `"d"`.

It is a 1→0|1 stage (every input row becomes zero or one output row) and runs in
declaration order like any other transform.

### Config fields and defaults

| Field | Default | Purpose |
|-------|---------|---------|
| `op_field` | `op` | Envelope field holding the operation code |
| `after_field` | `after` | Envelope field holding the post-image |
| `before_field` | `before` | Envelope field holding the pre-image |
| `key_field` | `document_key` | Fallback key for deletes with no `before` (MongoDB) |
| `marker_field` | `__op` | Field stamped on every emitted row (`"d"` / `"u"`) |
| `delete_ops` | `["d", "delete"]` | `op` values that mean delete |
| `drop_ops` | `["ddl", "truncate"]` | `op` values dropped entirely |

The defaults span all three CDC vocabularies seen in the wild — `insert` /
`update` / `delete` / `truncate`, `c` / `u` / `d` / `ddl`, and `c` / `u` / `r` /
`d` / `ddl` — so a bare `- type: cdc_unwrap` works for postgres-cdc, mysql-cdc,
and mongodb-cdc without per-source tuning.

`cdc_unwrap` is a built-in transform gated on the `transform-cdc-unwrap` feature
(included in the `full` build). It is **opaque** for column-lineage analysis (it
reshapes the whole envelope), so faucet emits no column-lineage edges for it.

See the [Upsert / mirror tables](./upsert.md) cookbook for the full
CDC → mirror pipeline.
