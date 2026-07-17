# ADR 0010 — A lean core library with orchestration in the CLI layer

*`faucet-core` stays a small, embeddable library; all scheduling, HTTP, DAG, and cluster logic lives above it in the CLI.*

- **Status:** Accepted (implemented) — `faucet-core` (library) vs `faucet-cli` (`expand`, `executor`, `serve/`, `schedule/`, `replication/`, `backfill/`).

## Context

faucet-stream ships both a reusable Rust library (`faucet-core`, depended on by 60+
connectors and by third parties) and a rich runtime (`faucet` CLI) with cron
scheduling, an HTTP control plane, a matrix DAG executor, snapshot→CDC replication,
backfill, and clustering. These two roles have very different change rates and
audiences.

## Problem

Where does orchestration live? If scheduling, `serve`, clustering, and DAG expansion
go into `faucet-core`, then every connector crate and every third-party embedder
inherits axum, croner, cluster state machines, and their churn — and the crate that
must stay stable becomes the crate that changes most.

## Decision

**Keep `faucet-core` a library that knows how to move one source to one sink and
checkpoint safely — and nothing else.** All orchestration is CLI-layer code built on
`expand` (config → `Vec<ExpandedNode>`) + `executor::run_expanded`, which drive
`Pipeline`:

| Concern | Lives in |
|---|---|
| move + checkpoint + passes + delivery | `faucet-core` (`Pipeline`, `run_stream`) |
| matrix DAG, gates, fan-out, cancellation | `cli/src/{expand,executor}.rs` |
| cron scheduling | `cli/src/schedule/` |
| HTTP control plane, RBAC, history, cluster | `cli/src/serve/` |
| snapshot→CDC handoff | `cli/src/replication/` |
| windowed historical replay | `cli/src/backfill/` |

Every long-running mode is the same `expand` + `run_expanded` wrapped in a driver
(see [execution](../architecture/execution.md)). The only `faucet-core` changes these
runtimes needed were tiny, defaulted trait methods (e.g. `capture_resume_position`
for replication) — never orchestration logic.

## Alternatives considered

- **A monolithic runtime in `faucet-core`.** Rejected: forces heavy dependencies and
  constant churn onto every connector and embedder; couples the stable contract to
  the fast-moving runtime.
- **A separate `faucet-runtime` crate between core and CLI.** Plausible, and may
  happen if a second front-end (beyond the CLI) ever needs the orchestration. Not
  done yet because the CLI is the only consumer; premature extraction would add a
  crate boundary with no second client.

## Trade-offs

- A third-party embedding `faucet-core` gets the safe move/checkpoint engine but must
  build its own orchestration (or vendor the CLI's). This is the intended split —
  most embedders want exactly the engine.
- Orchestration features can only be delivered through the CLI, not the library.

## Consequences

- **Positive:** `faucet-core` stays small, stable, and dependency-light; orchestration
  evolves fast without touching connectors; third parties embed a lean engine.
- **Negative:** no reusable orchestration crate for non-CLI embedders (yet); CLI-only
  features (`serve`, `schedule`) are invisible to library users by design.

## Future work

- Extract a `faucet-runtime` crate if a second orchestration front-end appears.

## Related

- [Execution model](../architecture/execution.md) · [pipeline](../architecture/pipeline.md) · [extensibility](../architecture/extensibility.md)
- [ADR 0006 — State management](./0006-state-management.md)
