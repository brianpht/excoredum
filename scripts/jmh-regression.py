#!/usr/bin/env python3
"""Compare the latest JMH results against a committed baseline and fail on a
regression greater than 10% on any reported percentile (tail percentiles are the
contract, per the performance budget). Records the current run as the baseline
with --record-baseline.

Usage:
  python3 scripts/jmh-regression.py                # run quickBench and check
  python3 scripts/jmh-regression.py --record-baseline  # run and write baseline
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RESULTS = REPO / "exc-core" / "build" / "results" / "jmh" / "results.json"
BASELINE = REPO / "config" / "jmh-baseline.json"
REGRESSION_LIMIT_PCT = 10.0


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def index_benchmarks(data):
    return {b["benchmark"]: b for b in data}


def metric_percentiles(bench):
    primary = bench.get("primaryMetric", {})
    percentiles = dict(primary.get("scorePercentiles", {}))
    if primary.get("score") is not None:
        percentiles["score"] = primary["score"]
    return percentiles


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--record-baseline", action="store_true")
    args = parser.parse_args()

    subprocess.run(["./gradlew", ":exc-core:jmh", "-PquickBench"], cwd=REPO, check=True)
    results = load(RESULTS)

    if args.record_baseline:
        BASELINE.parent.mkdir(parents=True, exist_ok=True)
        BASELINE.write_text(json.dumps(results, indent=2), encoding="utf-8")
        print(f"wrote baseline to {BASELINE}")
        return 0

    if not BASELINE.exists():
        print(f"no baseline at {BASELINE}; run with --record-baseline first", file=sys.stderr)
        return 2

    baseline = index_benchmarks(load(BASELINE))
    failed = False
    for bench in results:
        name = bench["benchmark"]
        base = baseline.get(name)
        if base is None:
            print(f"NEW BENCHMARK (no baseline): {name}")
            continue
        for key, new in metric_percentiles(bench).items():
            old = metric_percentiles(base).get(key)
            if old is None or new is None or old == 0:
                continue
            delta_pct = (new - old) / old * 100.0
            if delta_pct > REGRESSION_LIMIT_PCT:
                print(f"REGRESSION {name} {key}: {old:.4f} -> {new:.4f} ({delta_pct:+.1f}%)")
                failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
