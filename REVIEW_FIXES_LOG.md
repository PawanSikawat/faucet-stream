# Review Fixes Log

Running log of every change made on branch `chore/review-fixes-and-benchmarks`,
grouped by phase. Delete before the final commit if requested.

## Baseline

- Branch created: `chore/review-fixes-and-benchmarks` off `main` (582f6c3).
- Toolchain: rustup 1.96.0 (pinned; Homebrew rustc shadows PATH — invoked via
  `~/.rustup/toolchains/1.96.0-aarch64-apple-darwin/bin/{cargo,rustc}`).
- Baseline `cargo build --workspace`: _see build log / final summary_.
- Baseline `cargo test`: _see final summary_.

---

## Phase 1 — Reachability quick wins

### Findings
- **Repo metadata is empty** — `gh repo view` shows `description: ""`,
  `homepageUrl: ""`, `repositoryTopics: null`. This is the real discoverability
  gap. Fix requires `gh repo edit` → **AWAITING APPROVAL** (see proposed commands
  below).
- **Root `README.md` links: clean.** Every relative target resolves (verified all
  61 relative links point at existing files). Guide (absolute), docs.rs
  (absolute), and `cli/README.md` (CLI grammar) links are all present in the first
  ~55 lines (badges + "Documentation:" line + Quickstart). No dead links, nothing
  to fix.
- **Crate READMEs use `../`-style relative links** (7 files, e.g.
  `../../../LICENSE-APACHE`). These resolve correctly on crates.io because
  crates.io resolves README relative links against the README's path in the source
  repo. Left as-is (not broken).

### Proposed `gh` commands (NOT YET APPLIED — awaiting approval)

```bash
gh auth switch --user PawanSikawat

gh repo edit PawanSikawat/faucet-stream \
  --description "The fast, config-driven way to move data in Rust — a native CLI and an embeddable Rust ETL library" \
  --homepage "https://pawansikawat.github.io/faucet-stream/"

gh repo edit PawanSikawat/faucet-stream \
  --add-topic rust \
  --add-topic etl \
  --add-topic elt \
  --add-topic data-engineering \
  --add-topic data-pipeline \
  --add-topic cdc \
  --add-topic connectors \
  --add-topic data-integration \
  --add-topic streaming \
  --add-topic cli

gh auth switch --user pawan-dt
```

### Changes applied in this phase
- None to tracked source files (README verified clean; metadata pending approval).
- Added this `REVIEW_FIXES_LOG.md`.

---

## Phase 2 — Claim precision ("exactly-once" overclaim + superlatives)

### Principle
- The mechanism is **effectively-once** (idempotent at-least-once): per-page monotonic
  commit tokens committed atomically with the data + resume-and-skip dedup. Not
  distributed-consensus exactly-once.
- **Config identifier `delivery: exactly_once` is public API and was NOT renamed**
  (would break every user config). Only *prose* was reframed. The hyphenated form
  `exactly-once` is prose; the underscore form `exactly_once` is the config value —
  a hyphen-only replacement leaves the API untouched.

### Changes — README.md (root)
| Before | After |
|---|---|
| `…exactly-once delivery, upsert/delete…` (feature bullet) | `…effectively-once delivery (idempotent dedup-on-resume)…` |
| Capability row **"Exactly-once delivery"** / "…no duplicates on resume." | **"Effectively-once delivery"** + "…idempotent at-least-once (dedup on resume), not distributed-consensus exactly-once." |
| Comparison row `Exactly-once delivery \| ✓ (SQL/Iceberg/BigQuery)` | `Effectively-once delivery³ \| ✓ …` + new footnote ³ defining the term and linking the guide |
| "…CDC, exactly-once delivery, data-quality…" (when-to-use) | "…CDC, effectively-once delivery, data-quality…" |
| BigQuery sink row "exactly-once via MERGE" | "effectively-once via MERGE" |
| **Superlative** "Every connector is built to be **the fastest way to move its data in Rust**." | "**Built for throughput** … Throughput is a first-class design goal … (See `BENCHMARKS.md` — numbers, not adjectives.)" |

### Changes — faucet-stream/README.md
- "replication bookmarks / exactly-once tokens" → "…/ effectively-once commit tokens"
- example comment "with exactly-once + durable state" → "with effectively-once delivery + durable state"
- troubleshooting row "Exactly-once or CDC bookmarks lost…/ Exactly-once also requires…" → "Effectively-once…"

### Changes — docs/book (canonical explanation + sweep)
- `cookbook/state.md`: heading `## Exactly-once delivery` → `## Effectively-once delivery`;
  added a **precise-guarantee + failure-mode-boundary callout** (what it is/isn't, the
  crash-between-write-and-bookmark case, why the sink list is restricted to transactional
  targets). Kept the one intentional contrast phrase "not distributed-consensus exactly-once".
- Controlled replacement of hyphenated `exactly-once`→`effectively-once` (case-preserving)
  across the remaining user-facing markdown: `cookbook/{replication,contracts,upsert,cluster,resilience,schema-drift}.md`,
  `reference/{connectors,config}.md`, **all crate READMEs**, `cli/README.md`, `examples/README.md`.
  This also repointed the intra-doc anchor links `…/state.md#exactly-once-delivery` →
  `#effectively-once-delivery` to match the renamed heading (verified 0 dangling).
- Config values `delivery: exactly_once` left intact everywhere (verified underscore form preserved).

### Deliberately NOT changed (documented scope boundary)
- **84 `.rs` code/doc comments** referencing `exactly-once`: internal implementation +
  doc-comments accurately describing the `DeliveryMode::ExactlyOnce` API primitive (the
  identifier legitimately contains the term). Not marketing claims; no crates.io package
  description overclaims (verified: 0 `Cargo.toml` descriptions contain the phrase). The
  public *guarantee* is now precise in README + state.md.
- Per-crate `CHANGELOG.md` files (release-plz-managed history — one accidental edit reverted).
- "fastest way to start / confirm / see" colloquial UX phrasings in the guide (not throughput claims).
