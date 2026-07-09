# Benchmarks — faucet-stream vs Meltano (Singer)

Honest, reproducible evidence for the "built for throughput" claim. This compares
`faucet-stream` against [Meltano](https://meltano.com/) (the most common
[Singer](https://www.singer.io/) runtime) on an **identical** workload, on one
machine, for a batch file/DB → JSONL move.

**Regenerate everything** with `make bench` (1M rows) or `make bench-smoke` (100k).
The harness lives in [`benchmarks/`](benchmarks/README.md) and never fabricates a
number — a tool that won't install/run is recorded as such, not faked.

> **Read the caveats section before quoting any number.** This measures
> single-machine batch throughput of a specific move. It does **not** measure
> distributed throughput, connector breadth, or correctness.

## Methodology

### Dataset (deterministic)

`scripts/gen_bench_data.py` emits a seeded CSV (`--seed 42`), so both tools consume
byte-identical input and re-runs are comparable. Columns exercise mixed types — an
`int` id, two strings, a float, an RFC3339 timestamp, a bool, and a small nested
JSON column (`attributes`):

```
id,first_name,country,amount,created_at,active,attributes
1,Eve,GB,6767.32,2020-01-01T00:01:22Z,false,"{""tier"":""free"",""score"":2.501,""tags"":[""beta"",""epsilon""]}"
```

Sizes: **1,000,000 rows** (primary) and **100,000 rows** (smoke). On-disk sizes are
recorded per run in `benchmarks/results/versions.txt` (the 100k CSV is ~11.5 MiB;
the 1M CSV is ~10× that).

### Scenarios

| Scenario | faucet | Meltano |
|---|---|---|
| **A — CSV → JSONL** (no infra, always run) | `source-csv` → `sink-jsonl` | `tap-csv` → `target-jsonl` |
| **B — Postgres → JSONL** (needs Docker) | `source-postgres` → `sink-jsonl` | `tap-postgres` → `target-jsonl` |

### Measurement

- **Wall-clock**: [`hyperfine`](https://github.com/sharkdp/hyperfine), 1 warmup run
  discarded + **5 timed runs**, median ± stddev.
- **Peak RSS**: `/usr/bin/time` (`-l` on macOS, `-v` on Linux), one sampled run.
- **Throughput**: `rows ÷ median wall-clock`.
- **Parity**: both tools must emit the same row count; record shapes are
  spot-checked (see caveats re: Singer metadata).

### Exact reproduction

```bash
# 1. Build the release binary the harness uses
cargo build -p faucet-cli --release \
  --no-default-features --features "source-csv,sink-jsonl,source-postgres"

# 2. Run (installs an isolated Meltano venv on first run)
scripts/run-bench.sh            # 1,000,000 rows, 5 runs
scripts/run-bench.sh --smoke    # 100,000 rows

# Results land in benchmarks/results/{versions.txt,results.md,raw/*.json}
```

Tool versions and hardware are captured per run in
`benchmarks/results/versions.txt`. The run reported below used:

| | |
|---|---|
| Hardware | Apple M3 Pro, 12 cores, 18 GiB RAM |
| OS | macOS 26.5 (arm64) |
| Rust | rustc 1.96.0 |
| faucet | 1.2.0 (release: `source-csv,sink-jsonl,source-postgres`) |
| hyperfine | 1.19.0 |
| Meltano | 4.2.1 (Python 3.12.11) |
| Meltano plugins | `tap-csv` v1.3.2 (git, MeltanoLabs), `target-jsonl` 0.1.4 |
| Date | 2026-07-09 |

_See `benchmarks/results/versions.txt` for the exact `pip freeze` and dataset size
of the run reported below._

## Results

### Scenario A — CSV → JSONL

**Primary run — 1,000,000 rows** (116 MiB CSV, seed 42, 1 warmup + 5 timed runs):

| Tool | Median wall-clock (s) | Stddev (s) | Throughput (rows/s) | Peak RSS (MiB) | Rows out |
|---|---|---|---|---|---|
| **faucet-stream** | **1.40** | 0.015 | **712,403** | **11.8** | 1,000,000 |
| Meltano (Singer) | 135.46 | 6.03 | 7,383 | 724.5 | 1,000,000 |

On this workload and hardware, faucet-stream was **~96× faster** (712k vs 7.4k
rows/s) and used **~62× less peak memory** (11.8 vs 724 MiB), with **exact
row-count parity** (1,000,000 = 1,000,000).

**Smoke run — 100,000 rows** (11.5 MiB CSV, seed 42, 1 warmup + 5 timed runs):

| Tool | Median wall-clock (s) | Stddev (s) | Throughput (rows/s) | Peak RSS (MiB) | Rows out |
|---|---|---|---|---|---|
| **faucet-stream** | **0.18** | 0.011 | **548,246** | **11.8** | 100,000 |
| Meltano (Singer) | 18.92 | 1.78 | 5,286 | 306.4 | 100,000 |

Note how faucet's throughput *rises* from the 100k to the 1M run (548k → 712k
rows/s) as fixed startup cost amortizes, while Meltano's stays flat (~5–7k rows/s)
— consistent with a fixed Python/subprocess startup cost plus a per-record
Singer-over-pipe overhead. This is why the 1M run is the meaningful one.

### Scenario B — Postgres → JSONL

**1,000,000 rows** (`bench` table, seed 42, 1 warmup + 5 timed runs). Postgres 16
in Docker (colima); the same machine as Scenario A. faucet `source-postgres` →
`sink-jsonl` (streamed via `sqlx`) vs Meltano `tap-postgres` → `target-jsonl`.

| Tool | Median wall-clock (s) | Stddev (s) | Throughput (rows/s) | Peak RSS (MiB) | Rows out |
|---|---|---|---|---|---|
| **faucet-stream** | **5.56** | 0.110 | **179,700** | **13.9** | 1,000,000 |
| Meltano (Singer) | 139.19 | 1.595 | 7,184 | 743.0 | 1,000,000 |

On this workload/hardware, faucet-stream was **~25× faster** and used **~53× less
peak memory**, with **exact row-count parity** (1,000,000 = 1,000,000). The gap is
narrower than Scenario A because both tools are now bounded by real typed row
decoding from Postgres (Scenario A was all-string CSV) — faucet still streams
rows via `sqlx` while `tap-postgres` marshals each row through SQLAlchemy and the
Singer JSON pipe.

Reproduce with Docker: see
[`benchmarks/README.md`](benchmarks/README.md#scenario-b--postgres--jsonl-manual-needs-docker).

## Caveats — what this does and does NOT prove

**What it measures:** single-machine, single-process **batch throughput** and peak
memory for one CSV/DB → JSONL move on the hardware in `versions.txt`.

**What it does not prove / honest boundaries:**

1. **Not distributed.** This is one box, one process. It says nothing about
   horizontal scaling, multi-worker throughput, or cluster behavior.
2. **Python interpreter startup is part of Meltano's cost — and it's real, but
   call it out separately.** A meaningful slice of Meltano's wall-clock on smaller
   datasets is Python + plugin subprocess startup and the Singer JSON-over-pipe
   handoff between tap and target. That cost is *fixed*, so it dominates small runs
   and amortizes on large ones — which is exactly why the **1M-row run is the one
   that matters** and the 100k run is only a smoke test. faucet's compiled-binary
   startup advantage is genuine but is partly a startup effect; the 1M row is where
   steady-state streaming throughput shows through.
3. **Singer may add metadata.** Singer/target-jsonl can emit `_sdc_*` metadata
   columns, changing record shape and output size. In *this* configuration the
   record counts matched exactly and no `_sdc_*` columns were added, but a
   different tap/target pairing may differ — so treat row-count parity, not
   byte-size parity, as the correctness check.
4. **Both emit string-typed values here.** `tap-csv` emits all-string fields and
   faucet's CSV source likewise passes through string cells, so neither tool is
   doing type inference in Scenario A — this is a fair, apples-to-apples parse +
   reserialize. Scenario B (Postgres) exercises real typed decoding on both sides.
5. **Hardware-dependent.** Absolute numbers only mean something relative to the
   `versions.txt` machine. Re-run on your own hardware before quoting.
6. **One workload.** A different shape (wide rows, deeply nested JSON, many small
   files, network-bound APIs) could move the ratio. This is not a general
   "faucet is Nx faster" claim — it is one reproducible data point.

**Bottom line:** the comparison is designed to be *fair and falsifiable*, not
flattering. If faucet does not win on the 1M-row run on your hardware, the harness
will say so.
