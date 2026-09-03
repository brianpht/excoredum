#!/usr/bin/env python3
"""Compare the latest JMH results against a committed baseline.

Gate policy (see docs/decisions/performance-budget.md):
  - STRICT: the mean score (ns/op) is the stable metric; a regression greater
    than 10% fails the build.
  - ADVISORY: tail percentiles are noisy under the quick CI run, so they are
    reported but never fail the build.
  - --gc runs the JMH allocation profiler and strictly enforces the
    zero-allocation contract (allocation is deterministic, not timing-noisy).

Usage:
  python3 scripts/jmh-regression.py                    # run quickBench and check
  python3 scripts/jmh-regression.py --record-baseline  # run and write baseline
  python3 scripts/jmh-regression.py --gc               # assert zero allocation
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RESULTS = REPO / "core" / "build" / "results" / "jmh" / "results.json"
BASELINE = REPO / "config" / "jmh-baseline.json"
REGRESSION_LIMIT_PCT = 10.0
# Allocation is deterministic; allow a sub-byte epsilon only for JMH float rounding.
ALLOC_LIMIT_BYTES = 1.0

# All core benchmarks are time-based (AverageTime / SampleTime, ns/op, lower is
# better), so a positive delta is a regression. If a throughput-mode benchmark
# (ops/s, higher is better) is ever added, invert its score delta sign here.


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def bench_key(bench):
    return (bench["benchmark"], bench.get("mode", ""))


def index_benchmarks(data):
    return {bench_key(b): b for b in data}


def label(bench):
    name, mode = bench_key(bench)
    return f"{name} [{mode}]" if mode else name


def metric_values(bench):
    """Return {key: (severity, value)}: the mean score is strict, tail
    percentiles are advisory."""
    primary = bench.get("primaryMetric", {})
    values = {}
    if primary.get("score") is not None:
        values["score"] = ("strict", primary["score"])
    for pctl, val in primary.get("scorePercentiles", {}).items():
        values.setdefault(pctl, ("advisory", val))
    return values


def alloc_norm(bench):
    secondary = bench.get("secondaryMetrics", {})
    metric = secondary.get("\u00b7gc.alloc.rate.norm") or secondary.get("gc.alloc.rate.norm")
    return None if metric is None else metric.get("score")


def run_jmh(extra_args):
    subprocess.run(["./gradlew", ":core:jmh", "-PquickBench", *extra_args], cwd=REPO, check=True)


def check_regression(results, baseline):
    strict_failed = False
    for bench in results:
        base = baseline.get(bench_key(bench))
        if base is None:
            print(f"NEW BENCHMARK (no baseline): {label(bench)}")
            continue
        base_vals = metric_values(base)
        for key, (severity, new) in metric_values(bench).items():
            entry = base_vals.get(key)
            if entry is None or entry[1] in (None, 0) or new is None:
                continue
            old = entry[1]
            delta_pct = (new - old) / old * 100.0
            if delta_pct <= REGRESSION_LIMIT_PCT:
                continue
            if severity == "strict":
                print(f"REGRESSION (strict) {label(bench)} {key}: {old:.4f} -> {new:.4f} ({delta_pct:+.1f}%)")
                strict_failed = True
            else:
                print(f"WARN (advisory tail) {label(bench)} {key}: {old:.4f} -> {new:.4f} ({delta_pct:+.1f}%)")
    return 1 if strict_failed else 0


def check_alloc(results):
    failed = False
    seen = False
    for bench in results:
        norm = alloc_norm(bench)
        if norm is None:
            continue
        seen = True
        if norm >= ALLOC_LIMIT_BYTES:
            print(f"ALLOCATION {label(bench)}: {norm:.2f} B/op (violates zero-allocation contract)")
            failed = True
        else:
            print(f"ok {label(bench)}: {norm:.2f} B/op")
    if not seen:
        print("no gc.alloc.rate.norm metrics found; was the gc profiler attached?", file=sys.stderr)
        return 2
    return 1 if failed else 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--record-baseline", action="store_true")
    parser.add_argument(
        "--gc",
        action="store_true",
        help="run with the GC allocation profiler and assert the zero-allocation contract",
    )
    args = parser.parse_args()

    if args.gc:
        run_jmh(["-Pjmh.profilers=gc"])
        return check_alloc(load(RESULTS))

    run_jmh([])
    results = load(RESULTS)

    if args.record_baseline:
        BASELINE.parent.mkdir(parents=True, exist_ok=True)
        BASELINE.write_text(json.dumps(results, indent=2), encoding="utf-8")
        print(f"wrote baseline to {BASELINE}")
        return 0

    if not BASELINE.exists():
        print(f"no baseline at {BASELINE}; run with --record-baseline first", file=sys.stderr)
        return 2

    return check_regression(results, index_benchmarks(load(BASELINE)))


if __name__ == "__main__":
    sys.exit(main())
