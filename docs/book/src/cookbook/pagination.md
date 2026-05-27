# Pagination styles (REST source)

The REST source walks multi-page responses automatically. Set `pagination.type`
to one of the styles below. `max_pages` is a hard cap across all of them, and
every style has a loop/termination guard so a misbehaving API can't loop forever.

| Style | Stops when |
|-------|-----------|
| `None` | after the first page |
| `Cursor` | the next-token JSONPath is null/absent (or repeats) |
| `PageNumber` | a page returns zero records (or an identical body repeats) |
| `Offset` | the offset reaches `total` (via `total_path`) or a short page arrives |
| `LinkHeader` | there's no `rel="next"` in the `Link` response header |
| `NextLinkInBody` | the next-page URL in the body is absent, null, or empty |

## Cursor

```yaml
pagination:
  type: Cursor
  cursor_path: $.meta.next_cursor      # JSONPath to the next-page token
  cursor_param: cursor                 # query param to send it back as
```

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
