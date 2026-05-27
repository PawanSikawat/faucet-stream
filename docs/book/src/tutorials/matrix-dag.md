# Multi-pipeline DAGs with `matrix`

A single config can drive many pipeline invocations. The `matrix:` block lists
rows that are each deep-merged onto the base `pipeline:`. Rows can be independent
(fan-out) or form a parent/child **DAG** where a child runs once per record the
parent produced.

## Independent fan-out

Each row overrides part of the pipeline and runs independently, bounded by
`execution.max_concurrent`:

```yaml
version: 1
name: multi_region
pipeline:
  source: { type: rest, config: { base_url: https://api.example.com, method: GET } }
  sink:   { type: jsonl, config: {} }
execution:
  max_concurrent: 4
  on_error: continue   # or `stop`
matrix:
  - id: us
    source: { config: { path: /v1/us/events } }
    sink:   { config: { path: us.jsonl } }
  - id: eu
    source: { config: { path: /v1/eu/events } }
    sink:   { config: { path: eu.jsonl } }
```

## Parent/child DAG

A row with `parent:` runs once per record produced by the parent. Tokens like
`${parent_id.dotted.path}` are resolved per parent record at runtime:

```yaml
version: 1
name: dag_users_posts
pipeline:
  source: { type: rest, config: { base_url: https://api.example.com, method: GET, records_path: $.data[*] } }
  sink:   { type: jsonl, config: { append: false } }
matrix:
  # Root: fetch the users list once.
  - id: users
    source: { config: { path: /v1/users, name: users } }
    sink:   { config: { path: users.jsonl } }
  # Child: for each user record, fetch that user's posts.
  - id: posts
    parent: users
    parent_key: id
    source: { config: { path: /v1/users/${users.id}/posts, name: posts } }
    sink:   { config: { path: posts-${users.id}.jsonl } }
```

The child's state key is suffixed with the parent record's key, so each per-user
fetch resumes independently.

## Merge semantics

A row is deep-merged onto the base `pipeline`: scalars replace, objects merge
recursively, and arrays replace wholesale. That single rule defines all override
behavior.

## Named templates (DRY)

For many heterogeneous rows, define reusable source/sink templates under
`pipeline.sources` / `pipeline.sinks` and a top-level `vars:` block, then select
them per row with `ref:`. See [`cli/README.md`](https://github.com/PawanSikawat/faucet-stream/blob/main/cli/README.md)
for the full grammar.

## Error handling

`execution.on_error: continue` lets sibling subtrees finish when one fails (the
failed subtree is skipped); `stop` aborts pending and in-flight work on the first
failure. `stop` cancels in-flight tasks at their next `await`, which can leave
partial sink state — acceptable for idempotent sinks, something to know for
others.
