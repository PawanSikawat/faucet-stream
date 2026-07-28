# Airtable (via the REST source)

Airtable's REST API is a plain bearer-authenticated, offset-token-paginated
JSON API, so faucet's generic [`rest` source](../reference/connectors.md) covers
it end-to-end — **no dedicated connector crate is required** (issue
[#414](https://github.com/PawanSikawat/faucet-stream/issues/414)).

## How it maps onto the `rest` source

| Airtable concept | `rest` config |
|---|---|
| Personal access token (PAT) | `auth: { type: bearer, config: { token: ${env:AIRTABLE_TOKEN} } }` |
| Base + table URL | `base_url: https://api.airtable.com`, `path: /v0/<base_id>/<table>` |
| Records array | `records_path: "$.records"` |
| Offset-token pagination | `pagination: { type: Cursor, next_token_path: offset, param_name: offset }` |
| 5 req/s rate limit (HTTP 429 + `Retry-After`) | built-in retry: `max_retries` / `retry_backoff` |
| `view`, `filterByFormula`, `pageSize` | `query_params` |

Pagination stops automatically when the response omits `offset`. Each record is
`{ id, createdTime, fields: {…} }`; the `flatten` transform collapses `fields`
into dotted top-level keys (`fields.Name`, …).

## Runnable recipe

A complete, runnable config lives at
[`cli/examples/airtable_to_jsonl.yaml`](https://github.com/PawanSikawat/faucet-stream/blob/main/cli/examples/airtable_to_jsonl.yaml):

```yaml
version: 1
name: airtable_to_jsonl
vars:
  base_id: ${env:AIRTABLE_BASE_ID}
  table: Contacts
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.airtable.com
      path: /v0/${vars.base_id}/${vars.table}
      method: GET
      auth:
        type: bearer
        config:
          token: ${env:AIRTABLE_TOKEN}
      query_params:
        pageSize: "100"
      records_path: "$.records"
      pagination:
        type: Cursor
        next_token_path: offset
        param_name: offset
      max_retries: 5
      retry_backoff: 1
      replication_method:
        type: FullTable
      primary_keys: ["id"]
  transforms:
    - type: flatten
  sink:
    type: jsonl
    config:
      path: ./out/airtable_contacts.jsonl
```

```bash
export AIRTABLE_TOKEN=pat...      # data.records:read scope
export AIRTABLE_BASE_ID=appXXXXXXXXXXXXXX
faucet run cli/examples/airtable_to_jsonl.yaml
```

## Notes

- **Field types** — attachments and linked records come back as JSON arrays;
  they pass through as-is.
- **Incremental** — Airtable has no server-side change cursor; use
  `filterByFormula` on a `Last Modified` field to narrow, or run full-table.
- **Writing back** — an Airtable *sink* is out of scope; this recipe is
  read-only.
