# Learn faucet-stream from scratch

*The whole architecture as a story — read top to bottom, one idea at a time. No prior knowledge assumed.*

<table>
<tr>
<td>🎓 <b>Beginner's guide</b><br/><sub>you're here — start from zero</sub></td>
<td><a href="./README.md">🏛 Architect reference →</a><br/><sub>the deep, subsystem-by-subsystem docs</sub></td>
</tr>
</table>

> **Two ways to read the architecture.** This page is the **learning path**: it
> builds the system up in the order you'd naturally discover it, in plain
> language. Every section ends with a **🔍 Go deeper** link that flips you to the
> matching **architect reference** page for the same topic. Read straight through
> the first time; follow the deep links when you want the full story.

---

## The one-sentence idea

**faucet-stream moves data from one place to another.**

That's it. Picture a kitchen faucet: water comes *from* a pipe (the **source**),
flows *through* the tap, and out *into* the sink (the **sink**). faucet-stream is
the tap. You tell it where the data comes from and where it goes, and it moves
the data — reliably, without losing or scrambling it.

```mermaid
flowchart LR
    SRC[(Source<br/>where data comes from)] -->|records| PIPE[faucet pipeline] -->|records| SNK[(Sink<br/>where data goes)]
```

Everything else in this project — pages, bookmarks, retries, exactly-once — exists
to make that one sentence true *even when things go wrong*. We'll add those ideas
one at a time. By the end you'll understand the whole architecture, and it'll feel
obvious.

---

## Chapter 1 — The two characters: Source and Sink

The entire system is built from just **two roles**:

- A **Source** knows how to **read** records from somewhere (a database, an API,
  a file, a queue).
- A **Sink** knows how to **write** records somewhere else.

A **connector** is just a Source or a Sink for one specific system — e.g.
`faucet-source-postgres` reads from PostgreSQL, `faucet-sink-bigquery` writes to
BigQuery. There are dozens, but they all speak the same two-role language, which
is why *any* source can feed *any* sink.

### What a record is

A record is just **JSON** (`serde_json::Value` in Rust terms). No schema
paperwork, no code generation — a row from a database, a JSON object from an API,
a line from a file, they all become plain JSON objects flowing through the pipe.
That choice keeps connectors simple to write.

### The smallest possible connector

A Source, at its simplest, is one function: *"give me your records."*

```rust
// A Source implements one required method:
async fn fetch_with_context(&self, ctx) -> Result<Vec<Value>, FaucetError> {
    // …talk to the system, return a list of JSON records
}
```

A Sink is also one function: *"here are some records, write them."*

```rust
// A Sink implements one required method:
async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
    // …write the records, return how many you wrote
}
```

That's a working connector. Everything else a connector *can* do (streaming,
resuming, exactly-once…) is optional and added later — you'll see how in the next
chapters.

> 🔍 **Go deeper:** [Connector SDK](./connector-sdk.md) — the full `Source` /
> `Sink` trait contracts and why they're shaped this way.

---

## Chapter 2 — Moving data once (the normal fetch)

Now connect a Source to a Sink. That connection is the **Pipeline**:

```rust
let result = Pipeline::new(&source, &sink).run().await?;
println!("wrote {} records", result.records_written);
```

What happens under the hood, in the simplest case:

```mermaid
flowchart LR
    A["source.fetch — read ALL records"] --> B["sink.write_batch — write them"] --> C["done: records_written"]
```

Read everything, write everything, report how many. For a one-time copy — "move
this table into that warehouse once" — this is genuinely all you need. You now
understand the *happy path* of the whole tool.

But two real-world problems break this simple version, and fixing them is where
the interesting architecture comes from:

1. *"I don't want to re-copy everything every time I run."* → **Chapter 3.**
2. *"My dataset is too big to hold in memory."* → **Chapter 4.**

> 🔍 **Go deeper:** [Pipeline engine](./pipeline.md) — the `Pipeline` type and the
> loop that drives every run.

---

## Chapter 3 — Only the new stuff (incremental fetch)

Say you sync a table every hour. You don't want to re-read all 10 million rows
each time — just the ones that changed since last time. How does the tool
*remember* where it got to?

With a **bookmark**.

A bookmark is a small note the Source leaves for itself: *"I got up to here."* It
might be the largest `updated_at` timestamp it saw, or a database log position, or
a Kafka offset. On the next run, the Source reads that note and resumes from that
point instead of the beginning.

```mermaid
flowchart LR
    R1["run #1: read rows, remember bookmark = 'updated_at ≤ 09:00'"] --> S[(saved bookmark)]
    S --> R2["run #2: resume from 09:00, only read newer rows"]
```

Two small additions make a Source resumable:

- `state_key()` — a name for "my saved place."
- `apply_start_bookmark(bookmark)` — "resume from this note."

The note itself is stored in a **state store** (a file, Redis, or Postgres). The
Source produces the bookmark; the pipeline saves it; nobody else needs to
understand what's *inside* it.

### The one rule to remember (introduced gently)

Here's the single most important idea in the whole project, and it's
common-sense once you see it:

> **We save the bookmark only *after* the data is safely written.**

Why? Imagine we saved "I got up to row 1000" *first*, then crashed before actually
writing rows 900–1000. Next run we'd resume at 1000 and those rows would be **lost
forever**. So the order is always: **write the data → make sure it's really
saved → only then save the bookmark.** If we crash in between, the worst case is we
re-do a little work (safe), never that we skip data (catastrophic).

Keep this rule in your pocket — every advanced feature respects it.

> 🔍 **Go deeper:** [State management](./state-management.md) (how bookmarks are
> stored) and [Recovery](./recovery.md) (what happens after a crash).

---

## Chapter 4 — Datasets bigger than memory (streaming)

Chapter 2's "read everything into memory, then write everything" falls apart at a
billion rows — you'd run out of memory. The fix is to stop thinking about "all the
data" and start thinking in **pages**.

A **page** (`StreamPage`) is just a chunk of records — say 1,000 at a time — with
an optional bookmark attached. Instead of one giant read, the Source produces a
*stream* of pages, and the pipeline handles them one at a time:

```mermaid
flowchart LR
    P1[page 1] --> W1[write it] --> P2[page 2] --> W2[write it] --> P3[page 3 + bookmark] --> W3[write it] --> CK[flush + save bookmark]
```

Read a page, write a page, move on. Only one page is ever in memory, so it
doesn't matter whether the dataset is a thousand rows or a billion — memory stays
flat. This is **bounded memory by construction**, and it's the default behaviour.

Notice the bookmark from Chapter 3 rides along on the pages: whenever a page
carries a bookmark, the pipeline writes the page, makes sure it's durable, and
*then* saves the bookmark — exactly the rule from Chapter 3, now applied
per-page. Database change-feeds (CDC) use this to save a bookmark after every
transaction, so they can resume from the exact right spot.

And the best part: a connector author gets this for free. If they don't do
anything special, the pipeline automatically chops their `fetch` into pages. If
their system has a natural way to stream (a database cursor, the Kafka consumer),
they can plug that in for extra efficiency.

> 🔍 **Go deeper:** [Stream pages](./stream-pages.md) (the streaming contract) and
> [Batching](./batching.md) (page sizing and auto-tuning).

---

## Chapter 5 — The production toolbox (reach for these when you need them)

You now understand the **spine**: a source streams pages, the pipeline writes each
page and checkpoints safely, so you can resume after a crash. That's the whole
core — and it's all you need to move data.

Everything below is **optional**: a toolbox bolted onto the spine. Pull out each
tool the day you hit the problem it solves. The tools fall into a few families —
and the one almost every real pipeline reaches for, transforms, comes first.

### Shaping the data — transforms

Records rarely arrive in exactly the shape the destination wants, which is why
this is the most-used tool in the box. A **transform** rewrites each record as it
flows between the source and the sink — you don't write plumbing, the transform
just sits in the pipe.

```text
source ─▶ [ transform · transform · … ] ─▶ (validation) ─▶ sink
```

The everyday transforms are small and composable:

- **`flatten`** — collapse nested JSON into flat columns.
- **`select` / `drop`** — keep or remove fields.
- **`rename_field` / `keys_case`** — rename fields, or normalise their casing (snake / camel / …).
- **`cast`** — change a field's type (string → number, …), with a policy for bad values.
- **`redact` / `value_case` / `set`** — mask a value, change text case, or add a constant field.

Need real query power? The **SQL transform** runs an embedded DuckDB query over
each page — `SELECT … FROM batch` — so you can filter, join, or aggregate with
plain SQL. And when you're mirroring a database change-feed, **`cdc_unwrap`**
turns a raw CDC envelope (`{op, before, after}`) into a clean row plus a
delete/upsert marker, ready for the destination table.

Transforms layer at three levels — **pipeline-wide**, **per-source**, and
**per-row** (in a matrix) — and compose in that order, so shared shaping lives in
one place while a single row can still add its own tweak.

> 🔍 **Go deeper:** [Record transforms](../book/src/cookbook/transforms.md) ·
> [SQL transform](../book/src/cookbook/sql-transform.md) ·
> [Upsert / mirror tables](../book/src/cookbook/upsert.md) (the `cdc_unwrap` pairing).

### Guarding the data

| When you need to… | Reach for |
|---|---|
| Validate records and drop/quarantine the bad ones | **Quality checks** — [deep dive](./quality.md) |
| Promise downstream a stable, versioned output shape | **Contracts** — [deep dive](./contracts.md) |
| Never leak PII (runs *first*, before anything else sees it) | **Masking** — [deep dive](./masking.md) |
| React when the incoming shape drifts from the destination | **Schema drift** — [deep dive](./schema.md) |

### Moving it reliably

| When you need to… | Reach for |
|---|---|
| Keep going when a few rows fail, instead of aborting | **Dead-letter queue** — [deep dive](../book/src/cookbook/dlq.md) |
| Survive flaky networks (backoff, circuit breaker, poison-pill) | **Retries / resilience** — [retries](./retries.md), [resilience](./resilience.md) |
| Never write a row twice, even after a crash | **Exactly-once** — [deep dive](./recovery.md) |
| Keep a destination table mirrored (insert-or-update, deletes) | **Upsert / write modes** — [deep dive](../book/src/cookbook/upsert.md) |

### Getting data in and out at scale

| When you need to… | Reach for |
|---|---|
| Split one big source across workers | **Sharding** — [deep dive](../book/src/cookbook/cluster.md) |
| Bootstrap a table, then follow its changes with no gap | **Replication (snapshot → CDC)** — [deep dive](../book/src/cookbook/replication.md) |
| Replay a bounded historical window | **Backfill** — [deep dive](../book/src/cookbook/backfill.md) |
| Auto-generate configs from a live catalog | **Discovery** — [deep dive](../book/src/cookbook/discover.md) |
| Read/write compressed files transparently | **Compression** — [deep dive](../book/src/cookbook/compression.md) |

### Running & operating it

| When you need to… | Reach for |
|---|---|
| Run on a cron schedule | **Scheduling** — [deep dive](../book/src/cookbook/scheduling.md) |
| Run as a long-lived HTTP service | **Serve** — [deep dive](../book/src/cookbook/serve.md) |
| Spread runs across many machines | **Cluster** — [deep dive](../book/src/cookbook/cluster.md) |
| Kick off runs on events (object arrival, webhook, queue depth) | **Triggers** — [deep dive](../book/src/cookbook/triggers.md) |
| Fan one config into many pipelines (a DAG) | **Matrix + config composition** — [execution](./execution.md) |
| Pull credentials from a secrets manager | **Secrets** — [deep dive](../book/src/cookbook/secrets.md) |

### Seeing what happened

| When you need to… | Reach for |
|---|---|
| Metrics, traces, and OTLP export (automatic — no code) | **Observability** — [deep dive](./observability.md) |
| Track where data came from and went | **Lineage (OpenLineage)** — [deep dive](../book/src/cookbook/lineage.md) |
| Alert on staleness or volume anomalies | **SLA monitoring** — [deep dive](../book/src/cookbook/sla.md) |
| Browse every dataset a pipeline has touched | **Data Movement Catalog** — [deep dive](../book/src/cookbook/catalog.md) |
| Send Slack / PagerDuty / webhook alerts | **Notifications** — [deep dive](../book/src/cookbook/notifications.md) |

### How the per-page pieces fit together (still respecting Chapter 3's rule)

When several of the *data-guarding* tools are on, the pipeline runs them in a
fixed, safe order on each page — and the order is chosen to be *safe*, not
arbitrary:

```mermaid
flowchart LR
    PAGE[page arrives] --> MASK[1 mask PII] --> Q[2 quality] --> C[3 contract] --> D[4 schema drift] --> WRITE[5 write to sink] --> FLUSH[flush] --> CK[save bookmark]
```

Masking is first so PII can't leak anywhere. Validation happens before the write
so bad data never lands. And the bookmark is still saved *last* — the golden rule
from Chapter 3 never bends, no matter how many tools you add.

---

## The one rule that ties everything together

If you remember nothing else, remember this:

> **A bookmark is saved only *after* the sink has durably written and flushed the
> page.** Write → flush → checkpoint. Always.

Every chapter, every upgrade, every failure mode in this project is a consequence
of that single ordering. It's why a crash never loses data; it's why retries are
safe; it's why exactly-once works. When you read the architect reference and see
"the invariant," this is it.

> 🔍 **Go deeper:** [Design invariants](./invariants.md) — this rule and its
> siblings, written down formally with where each is enforced in the code.

---

## Where to go next

You've built the whole mental model. From here:

- **Keep learning the internals** → switch to the [🏛 Architect reference](./README.md)
  and read the spine in order (overview → execution → pipeline → stream-pages →
  state → recovery).
- **Build your own connector** → [Authoring a connector](../contributing/connector-authoring.md).
- **Actually run a pipeline** → the user guide's
  [first pipeline tutorial](../book/src/getting-started/first-pipeline.md).
- **Look up a term** → the [Glossary](../glossary.md).

## Related

- [Architect reference (home)](./README.md) · [Overview](./overview.md) · [Design invariants](./invariants.md)
- [Glossary](../glossary.md)
- User guide: [Core concepts](../book/src/getting-started/concepts.md) · [Your first pipeline](../book/src/getting-started/first-pipeline.md)
