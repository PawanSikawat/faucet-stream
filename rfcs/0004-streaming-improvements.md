# RFC 0004 — Streaming pipeline improvements

*Add optional read/write pipelining, explicit backpressure, and a push-source path to `stream_pages`, without weakening the checkpoint-ordering invariant.*

| | |
|---|---|
| **RFC** | 0004 |
| **Title** | Streaming pipeline improvements |
| **Status** | Draft (proposal) |
| **Authors** | faucet-stream maintainers |
| **Related issues** | epic #38 |
| **Related ADRs** | [0001 stream-pages](../docs/adr/0001-stream-pages.md), [0002 checkpoint ordering](../docs/adr/0002-checkpoint-ordering.md) |

## Summary

The streaming core (`run_stream` in `crates/core/src/pipeline.rs`) processes one
page at a time: poll a page, run the validation passes, write it, flush, persist
the bookmark, repeat. This is simple and correct, and it bounds memory at
O(batch_size). This RFC proposes three optional enhancements — cross-page
read/write pipelining, explicit backpressure signalling, and a push-based source
adapter — each additive and each preserving the write → flush → checkpoint
ordering.

## Motivation

The current loop is strictly sequential across pages. Grounded in the code:

- **No read/write overlap.** While the sink writes page *N*, the source sits
  idle; while the source fetches page *N+1*, the sink sits idle. For pipelines
  where read and write latency are comparable (a network source feeding a
  network sink), roughly half the wall-clock is avoidable stall.
- **Implicit backpressure only.** Backpressure today is the page cadence itself —
  the source cannot produce page *N+1* until the pipeline polls for it. That is
  safe but coarse; there is no way for a fast source to buffer a bounded amount
  ahead, nor for a slow sink to signal "slow down" beyond blocking the poll.
- **No push-source story.** `Source::stream_pages` is pull-based. Genuinely
  push-based inputs (a socket, a subscription, an in-process producer) are shoe-
  horned into the pull model (e.g. the webhook source buffers). Client-streaming
  and bidirectional gRPC are explicitly out of scope today.

The [batching architecture](../docs/architecture/batching.md) and
[ADR 0001](../docs/adr/0001-stream-pages.md) document today's model; this RFC is
the "future evolution" those docs point to.

## Guide-level explanation

Opt-in, per pipeline:

- **Pipelining (double-buffering).** The pipeline may prefetch the *next* page
  while the *current* page is being written, using a small bounded channel
  (depth configurable, default 1 = today's behaviour). Latency becomes
  `max(read, write)` per page instead of `read + write`.
- **Explicit backpressure.** The bounded channel *is* the backpressure: a fast
  source blocks when the buffer is full; a slow sink naturally throttles the
  source. This is the standard bounded-queue mechanism, made explicit and
  tunable rather than implicit.
- **Push sources.** A `Source` may adapt a push input by feeding a bounded
  channel that the pipeline drains as pages — turning push into pull at the
  channel boundary, with the same backpressure semantics.

The checkpoint-ordering invariant is untouched: a page's bookmark is still
persisted only after that page's write is flushed. Prefetching page *N+1* does
not persist its bookmark until page *N* is durable and page *N+1* itself is
written and flushed, in order.

## Reference-level explanation

- **Ordered prefetch.** Introduce an optional adapter that wraps the
  `stream_pages` stream in a bounded look-ahead buffer (e.g. `buffered`/a bounded
  channel of depth *k*). Crucially, **pages must remain ordered and committed in
  order** — bookmarks are monotonic and the exactly-once commit-token sequence
  (`next_seq`) assumes in-order commit. So this is *prefetch*, not out-of-order
  concurrency: page *N* is always written+flushed+checkpointed before page *N+1*.
- **Config surface.** A `streaming: { prefetch: <depth> }` knob (default 1). At
  depth 1 the code path is byte-for-byte today's sequential loop.
- **Invariant preservation.** The write/flush/checkpoint block in `run_stream`
  stays exactly as-is; only page *production* is decoupled from page
  *consumption* via the bounded buffer. The cancellation `select!` (biased,
  page-boundary flush) continues to work because cancellation is still checked
  per consumed page.
- **Push adapter.** Provide a core helper (a bounded-channel `Source` wrapper) so
  push inputs share one well-tested pull-conversion instead of each connector
  re-inventing buffering (the webhook source is the reference case). This dovetails
  with [RFC 0005](./0005-async-connector-runtime.md)'s shared runtime primitives.

## Drawbacks

- **Memory.** Prefetch depth *k* raises the memory bound to O(k · batch_size).
  Default depth 1 keeps today's bound; the knob makes the trade-off explicit.
- **Complexity around cancellation and errors.** A prefetched page that is never
  consumed (because an earlier page failed or the run was cancelled) must be
  dropped cleanly; the buffered future must be cancel-safe.
- **Exactly-once ordering constraint.** Prefetch must stay strictly ordered; any
  temptation toward out-of-order concurrency would break the monotonic
  commit-token/bookmark assumptions and is out of scope.
- **Diminishing returns for CDC.** CDC sources emit a bookmark per committed
  transaction and are often latency- not throughput-bound; prefetch helps
  paged/query sources most.

## Rationale and alternatives

- **Full parallel page processing (out-of-order).** Rejected — incompatible with
  monotonic bookmarks and the exactly-once sequence; would require a fundamentally
  different commit model. Sharding ([`Shardable`](../docs/architecture/connector-sdk.md))
  already provides *inter-shard* parallelism the safe way.
- **Do nothing.** Perfectly acceptable for correctness; leaves the read/write
  stall unaddressed for balanced-latency pipelines.
- **Unbounded prefetch.** Rejected — reintroduces the O(total) buffering that
  [ADR 0001](../docs/adr/0001-stream-pages.md) exists to prevent.

## Prior art

Bounded-channel backpressure is standard (`tokio::sync::mpsc`, Akka Streams,
Reactive Streams). Kafka Connect's `poll()`/`put()` loop with a worker buffer is
the direct analog. Flink's credit-based backpressure is the richer model we are
deliberately not adopting in v1.

## Unresolved questions

- Must resolve before Accepted: whether prefetch is a core `run_stream` option or
  a source-side stream adapter.
- During implementation: interaction with adaptive batching and with the
  per-transaction bookmark cadence of CDC sources.

## Future possibilities

- Credit-based backpressure metrics surfaced via the observability layer.
- First-class bidirectional/streaming gRPC once the push adapter exists.

## Related

- [RFC process](./README.md) · [RFC 0002 Arrow](./0002-arrow-support.md) · [RFC 0005 async runtime](./0005-async-connector-runtime.md)
- [Stream pages](../docs/architecture/stream-pages.md) · [Batching](../docs/architecture/batching.md) · [ADR 0002 checkpoint ordering](../docs/adr/0002-checkpoint-ordering.md) · [Design invariants](../docs/architecture/invariants.md)
