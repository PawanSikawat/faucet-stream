# Scope & Sustainability Proposal — connector support tiers

> **Status: proposal only. No code is removed by this document.** It recommends a
> *support-tier* split so a single maintainer can set honest expectations without
> deleting any connector. Everything currently shipped keeps shipping.

## The problem it addresses

faucet-stream ships **41 connectors** (23 sources + 18 sinks) plus a full control
plane (serve, cluster, triggers, scheduler, lineage, catalog). That is a large
surface for one maintainer. The risk is not that the code is bad — it is that
*every* connector implicitly carries the same "production-grade, actively
maintained" promise, and no single person can guarantee that uniformly across 41
external systems that each version on their own cadence. Setting explicit tiers
converts an implicit, unbounded promise into an honest, bounded one — which
*raises* trust rather than lowering it.

## Proposed tiers

### Tier 1 — Supported (maintainer-guaranteed)

The connectors that (a) cover the overwhelming majority of real pipelines, (b)
talk to stable, well-specified protocols, and (c) are the cheapest to keep green.
These get: CI integration tests kept passing, issues triaged first, semver care,
and a "Supported" badge.

| Connector | Rationale |
|---|---|
| `rest` (source) | The universal source; most-used, pure HTTP, no vendor SDK. |
| `postgres` (source + sink) | Ubiquitous OLTP; stable wire protocol via `sqlx`. |
| `mysql` (source + sink) | Ubiquitous OLTP; stable protocol. |
| `sqlite` (source + sink) | Zero-infra, fully testable in-process. |
| `csv` (source + sink) | Zero-infra, universal interchange. |
| `jsonl` (sink) + `stdout` (sink) | Zero-infra, the default landing formats. |
| `s3` (source + sink) | The de-facto object store; stable, widely mirrored API. |
| `bigquery` (source + sink) | Most-requested warehouse; already the most feature-complete sink (exactly-once + upsert). |
| `kafka` (source + sink) | The streaming backbone; stable via `rdkafka`. |
| `parquet` (source + sink) | Columnar interchange; stable Arrow/Parquet spec. |

### Tier 2 — Experimental / community

Fully shipped and usable, but explicitly **best-effort**: correctness-critical
bugs are still fixed, but breadth of testing, response time, and API-drift
tracking are not guaranteed. Community PRs especially welcome here. These get an
"Experimental" badge and a one-line note in each README + `#[doc]` header.

- **CDC**: `postgres-cdc`, `mysql-cdc`, `mongodb-cdc` — highest-value but
  protocol-sensitive (see drift list below); promote to Tier 1 individually once
  each has a Docker-based CI matrix pinned across server versions.
- **Warehouses / search**: `snowflake` (source + sink), `elasticsearch` (source + sink).
- **Cloud storage**: `gcs` (source + sink).
- **Other DBs**: `mssql` (source + sink), `mongodb` (source + sink), `redis` (source + sink).
- **API shapes**: `graphql`, `xml`, `grpc`, `webhook`, `websocket` (sources); `http` (sink).
- **Lakehouse**: `iceberg` (sink) — append-only today; catalog matrix is large.

## How to surface tiers **without deleting code**

1. **A `Support` column in the connector capability matrix**
   (`docs/book/src/reference/connectors.md`) — `Tier 1` / `Tier 2` per row. This is
   the single source of truth; it's already a table.
2. **A one-line README banner** at the top of each connector crate's `README.md`:
   `> **Support tier: Experimental.** Best-effort; correctness bugs fixed, breadth not guaranteed.`
   (Tier 1 crates get `> **Support tier: Supported.**`)
3. **A `#[doc]` header line** on each connector's public `Source`/`Sink` type so the
   tier shows on docs.rs, e.g.
   ```rust
   //! **Support tier: Experimental** — see the [support policy](https://pawansikawat.github.io/faucet-stream/reference/connectors.html#support-tiers).
   ```
4. **A root-README legend** mapping the tier badges, plus a short "Support policy"
   section in the guide defining exactly what each tier promises.
5. **Cargo keyword / category unchanged** — tiering is documentation + CI policy,
   not a packaging change, so no crate is yanked or renamed.

CI follow-through (optional, separate PR): mark the Tier-2 integration-test jobs
`continue-on-error` (they already largely are, being Docker-gated) and keep only
Tier-1 connectors' tests in the required set — so a flaky vendor API never reds
`main`, but Tier-1 stays a hard gate.

## Top 5 connectors most exposed to upstream API drift

Ranked by how often the *external* system changes in ways that can silently break
the connector (auth flows, protocol/version bumps, response-shape changes):

1. **`snowflake`** — the SQL REST API + JWT/OAuth key-pair auth and token-rotation
   semantics change relatively often; partition-pagination behavior is version-sensitive.
2. **`bigquery`** — auth (ADC/service-account), `jobs.query` vs streaming-insert
   quotas, and the multi-statement transaction surface used for exactly-once/upsert
   all evolve on Google's cadence.
3. **`postgres-cdc` / `mysql-cdc`** — logical-replication (`pgoutput`) and binlog
   formats shift across major server versions; resume/bookmark correctness is
   protocol-coupled and the failure mode is data loss, not a loud error.
4. **`elasticsearch`** — the bulk/search/scroll APIs diverged across ES 7→8→9 and
   again between Elasticsearch and the OpenSearch fork; auth modes multiplied.
5. **`gcs`** — Google Cloud Storage auth (ADC, workload identity) and the
   Storage/StorageControl client surfaces change independently of the data path.

(Honorable mention: `mongodb-cdc` change-stream resume-token semantics across
MongoDB server versions.)

## Recommendation

Adopt the two-tier split as **documentation + CI policy only** in a follow-up PR:
add the `Support` column, the README/`#[doc]` banners, and a "Support policy"
guide page; move Tier-2 integration tests out of the required CI set. Revisit
tier membership quarterly — a Tier-2 connector earns Tier 1 once it has a pinned,
green, version-matrixed integration test. No connector is ever deleted by this
policy; the long tail simply carries an honest label.
