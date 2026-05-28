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
| `keys_case` | Re-case every key (snake / camel / pascal / kebab / screaming_snake) | `mode` |
| `spell_symbols` | Spell out symbols in keys (`%` → `percent`, `#` → `number`, …) | `extra`, `separator` |
| `select` | Keep only listed top-level fields | `fields: [..]` |
| `drop` | Remove listed top-level fields | `fields: [..]` |
| `set` | Add or overwrite top-level fields with constants | `values: {k: v, ..}` |
| `rename_field` | Exact-name rename (vs. regex) | `fields: {from: to, ..}` |
| `cast` | Coerce per-field types | `fields: {name: type}`, `on_error` |
| `redact` | Replace listed field values with a mask | `fields: [..]`, `mask` |
| `value_case` | Lowercase / uppercase / trim string values | `fields: [..]`, `mode` |

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

## `keys_case` — pick the output convention

```yaml
- type: keys_case
  config:
    mode: snake   # | camel | pascal | kebab | screaming_snake
```

The tokeniser splits each key on whitespace, `_`, `-`, dropped
punctuation, and lower→upper transitions (so `"firstName"` and
`"first_name"` and `"first-name"` all tokenise the same), then re-joins
in the requested style:

| Input          | `snake`        | `camel`       | `pascal`     | `kebab`        | `screaming_snake` |
|----------------|----------------|---------------|--------------|----------------|-------------------|
| `"First Name"` | `first_name`   | `firstName`   | `FirstName`  | `first-name`   | `FIRST_NAME`      |
| `"last-name"`  | `last_name`    | `lastName`    | `LastName`   | `last-name`    | `LAST_NAME`       |
| `"camelCase"`  | `camel_case`   | `camelCase`   | `CamelCase`  | `camel-case`   | `CAMEL_CASE`      |
| `"ID"`         | `id`           | `id`          | `Id`         | `id`           | `ID`              |

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
normalises the output (so `+00:00` becomes `Z`).

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

## `value_case`

```yaml
- type: value_case
  config:
    fields: [email, username]
    mode: lower   # | upper | trim
```

Only string field values are touched; non-string values (numbers, bools,
nulls, nested objects) pass through unchanged.

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

- **Filter (1→0)** and **explode/unnest (1→N)** — these don't fit the
  `Fn(Value) -> Value` shape and need a richer transform model. Tracked
  in [#99].
- **Dotted-path field selection** — keep things simple: run `flatten`
  first, then operate on the flattened key.
- **A general expression / scripting transform (jq, CEL, …)** —
  separate, larger discussion.

[#99]: https://github.com/PawanSikawat/faucet-stream/issues/99
