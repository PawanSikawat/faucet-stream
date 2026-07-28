# faucet-source-sqs

AWS SQS **source** connector for
[faucet-stream](https://github.com/PawanSikawat/faucet-stream): long-polls
`ReceiveMessage`, buffers up to `batch_size` messages, and emits them page by
page with bounded memory. It terminates on `idle_timeout_secs` and/or
`max_messages`.

```yaml
source:
  type: sqs
  config:
    queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/events
    region: us-east-1
    idle_timeout_secs: 30      # at least one termination knob is required
    wait_time_seconds: 10
    batch_size: 1000
```

## Configuration

| Field | Default | Notes |
|-------|---------|-------|
| `queue_url` | — | Required. Full SQS queue URL. |
| `region` | SDK default chain | |
| `endpoint_url` | — | LocalStack / VPC endpoint override. |
| `credentials` | `{ type: default }` | `default` \| `profile` \| `access_key` \| `assume_role` \| `web_identity` — see `faucet-common-sqs`. |
| `idle_timeout_secs` / `max_messages` | — | **At least one is required** so a batch run terminates. |
| `wait_time_seconds` | `10` | Long-poll wait per `ReceiveMessage` (0–20). |
| `batch_size` | `1000` | Records per emitted page. `0` = one page for the whole drain. |

Each `ReceiveMessage` call requests up to 10 messages (the SQS API cap),
capped further so it never over-reads past `max_messages`.

## Record shape

Each message body is emitted as its **parsed JSON value** when the body is
valid JSON, otherwise as a JSON **string** of the raw body:

```json
{ "order_id": 42, "status": "shipped" }
```
```json
"a plain, non-JSON body"
```

## Delivery semantics

Each page's receipt handles are deleted (via `DeleteMessageBatch`) right before
the page is yielded. Delivery is **at-least-once**: any message whose delete
does not land — or whose visibility window elapses before a downstream commit —
is redelivered on a later run. There is no resumable bookmark (`bookmark: None`
on every page); the queue itself is the cursor. Key downstream consumers on a
message field (e.g. an upsert sink) when replays must converge.

## LocalStack

```yaml
config:
  endpoint_url: http://localhost:4566
  region: us-east-1
  credentials: { type: access_key, config: { access_key_id: test, secret_access_key: test } }
```

## License

MIT OR Apache-2.0
