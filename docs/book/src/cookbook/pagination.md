# Pagination styles (REST source)

The REST source walks multi-page responses automatically. Set `pagination.type`
to one of the styles below. `max_pages` is a hard cap across all of them, and
every style has a loop/termination guard so a misbehaving API can't loop forever.

| Style | Stops when |
|-------|-----------|
| `None` | after the first page |
| `Cursor` | the next-token JSONPath is null/absent (or repeats) |
| `CursorInBody` | the next-token JSONPath is null/absent (or repeats) — for POST-search APIs that take the cursor in the request body |
| `PageNumber` | a page returns zero records (or an identical body repeats) |
| `Offset` | the offset reaches `total` (via `total_path`) or a short page arrives |
| `LinkHeader` | there's no `rel="next"` in the `Link` response header |
| `NextLinkInBody` | the next-page URL in the body is absent, null, or empty |

> An HTTP **`204 No Content`** (or any 2xx with an empty body) is treated as an
> empty page under every style, so a feed that ends with a `204` after its last
> data page (e.g. ADP's `$top`/`$skip` paging) stops cleanly instead of erroring.

## Cursor

```yaml
pagination:
  type: Cursor
  next_token_path: $.meta.next_cursor  # JSONPath to the next-page token
  param_name: starting_after           # query param to send it back as
```

## Cursor in body (POST search)

For endpoints that page a **POST** search body — the next cursor comes back in
the response and must be written back into the request JSON body (e.g. HubSpot
CRM `POST …/objects/{obj}/search`):

```yaml
source:
  type: rest
  config:
    method: POST
    body: { limit: 100, sorts: ["hs_lastmodifieddate"] }   # base search body
    records_path: $.results[*]
    pagination:
      type: CursorInBody
      next_token_path: $.paging.next.after   # JSONPath to the next cursor
      body_cursor_field: after               # body field to inject it into
```

The first request sends `body` unchanged; each later request adds
`body[body_cursor_field] = <cursor>`. Pagination stops when the cursor is
null/absent or repeats.

## Page number

```yaml
pagination:
  type: PageNumber
  param_name: page
  start_page: 1
  page_size: 500
  page_size_param: per_page
```

## Offset / limit

```yaml
pagination:
  type: Offset
  limit: 1000
  limit_param: limit
  offset_param: offset
  total_path: $.meta.total             # optional; enables an exact stop
```

## Link header

```yaml
pagination:
  type: LinkHeader      # follows the RFC 5988 `Link: <…>; rel="next"` header
```

## Next link in body

```yaml
pagination:
  type: NextLinkInBody
  next_link_path: $.links.next         # JSONPath to the absolute next-page URL
```

> Use `faucet schema source rest` to see the exact fields and defaults for each
> style in your installed version.
