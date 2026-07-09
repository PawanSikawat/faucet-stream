#!/usr/bin/env python3
"""Deterministic, seeded synthetic dataset generator for faucet-stream benchmarks.

Emits a CSV with mixed types so the CSV→JSONL benchmark exercises real parsing,
not a single-column toy. Columns:

    id          int         monotonic 1..N
    first_name  string      from a small fixed pool
    country     string      ISO-ish code from a fixed pool
    amount      float       2-decimal value
    created_at  timestamp   RFC3339 (UTC), deterministic walk
    active      bool         true/false
    attributes  json string  small nested object: {"tier": .., "score": .., "tags": [..]}

The stream is fully determined by --seed and --rows, so two runs (and the two
tools under test) see byte-identical input. Nothing is randomised outside the
seeded PRNG.

Usage:
    python3 gen_bench_data.py --rows 1000000 --seed 42 --out data/bench_1m.csv
    python3 gen_bench_data.py --rows 100000  --seed 42 --out data/bench_100k.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
from datetime import datetime, timedelta, timezone

FIRST_NAMES = [
    "Ada", "Bo", "Chen", "Dinesh", "Eve", "Farah", "Gus", "Hana",
    "Ivan", "Juno", "Kato", "Lena", "Mira", "Nils", "Omar", "Priya",
]
COUNTRIES = ["US", "GB", "DE", "IN", "JP", "BR", "NG", "CA", "AU", "FR"]
TIERS = ["free", "pro", "enterprise"]
TAGS = ["alpha", "beta", "gamma", "delta", "epsilon"]

HEADER = ["id", "first_name", "country", "amount", "created_at", "active", "attributes"]


def gen_rows(rows: int, seed: int):
    rng = random.Random(seed)
    # Deterministic epoch start; each row advances the clock by a seeded step.
    base = datetime(2020, 1, 1, tzinfo=timezone.utc)
    t = base
    for i in range(1, rows + 1):
        t = t + timedelta(seconds=rng.randint(1, 90))
        attributes = {
            "tier": rng.choice(TIERS),
            "score": round(rng.uniform(0, 100), 3),
            "tags": rng.sample(TAGS, rng.randint(1, 3)),
        }
        yield [
            i,
            rng.choice(FIRST_NAMES),
            rng.choice(COUNTRIES),
            round(rng.uniform(1.0, 10000.0), 2),
            t.isoformat().replace("+00:00", "Z"),
            "true" if rng.random() < 0.5 else "false",
            json.dumps(attributes, separators=(",", ":")),
        ]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rows", type=int, required=True, help="number of data rows")
    ap.add_argument("--seed", type=int, default=42, help="PRNG seed (default 42)")
    ap.add_argument("--out", required=True, help="output CSV path")
    args = ap.parse_args(argv)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(HEADER)
        for row in gen_rows(args.rows, args.seed):
            w.writerow(row)

    size = os.path.getsize(args.out)
    print(
        f"wrote {args.rows} rows (seed={args.seed}) to {args.out} "
        f"({size} bytes, {size / 1_048_576:.1f} MiB)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
