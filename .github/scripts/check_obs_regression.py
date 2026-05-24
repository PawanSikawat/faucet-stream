#!/usr/bin/env python3
"""
Compare observability bench results against the baseline. Fails CI if
instrumented_with_recorder > baseline_no_decorator * 1.05 (5% threshold).

The threshold is a regression budget, not a zero-overhead claim: the `metrics`
facade has ~3-5% steady-state cost from macro dispatch even with a
DebuggingRecorder. The 5% gate catches genuine regressions (lock contention,
accidental clones, etc.) without flapping on micro-benchmark noise at
sample_size(10).
"""

import json
import sys
from pathlib import Path

ROOT = Path("target/criterion/observability")


def median(name: str) -> float:
    path = ROOT / name / "new" / "estimates.json"
    if not path.exists():
        print(f"missing estimates file: {path}", file=sys.stderr)
        sys.exit(2)
    return json.loads(path.read_text())["median"]["point_estimate"]


def main() -> int:
    try:
        baseline = median("baseline_no_decorator")
        instrumented_no_rec = median("instrumented_no_recorder")
        instrumented_with_rec = median("instrumented_with_recorder")
    except Exception as e:  # noqa: BLE001
        print(f"error reading criterion estimates: {e}", file=sys.stderr)
        return 2

    ratio_no_rec = instrumented_no_rec / baseline
    ratio_with_rec = instrumented_with_rec / baseline
    print(f"baseline                    = {baseline:.3e}")
    print(f"instrumented_no_recorder    = {instrumented_no_rec:.3e}  ratio={ratio_no_rec:.3f}")
    print(f"instrumented_with_recorder  = {instrumented_with_rec:.3e}  ratio={ratio_with_rec:.3f}")

    # The metrics facade has measurable but small overhead even with a
    # DebuggingRecorder (~3-5%). The threshold below catches genuine regressions
    # (accidental lock contention, double-clone, etc.) rather than the steady-state
    # cost of the metrics layer. Tighten if/when we want a stricter SLO.
    THRESHOLD = 1.05
    if ratio_with_rec > THRESHOLD:
        print(
            f"REGRESSION: instrumented_with_recorder exceeds baseline_no_decorator by >{(THRESHOLD - 1.0) * 100:.0f}%",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
