# Performance budget

This is the budget source of truth for justrade. It overrides the defaults in
`.github/copilot-instructions.md`. A regression greater than 10% on any
percentile must be rolled back or justified with a new ADR. Absolute ns/op is
only comparable within a single, controlled environment (same host hardware
and JDK build): a run on a shared CI runner is not a valid baseline target, so
cross-environment timing comparisons are advisory and never block the build.

Priority: Correctness > Determinism > Tail Latency > Mean Latency > Throughput.

## Core (matching engine and clustered service)

| Metric                          | Target            |
|---------------------------------|-------------------|
| Small message decode            | < 100 ns          |
| Ring buffer publish             | < 80 ns           |
| Primitive map lookup            | < 50 ns           |
| End-to-end IPC p50              | < 5 us            |
| End-to-end IPC p99              | < 15 us           |
| End-to-end IPC p99.99           | < 50 us           |
| Hot-path allocation             | 0 bytes / event   |
| GC pause during the operational window | 0 ms      |

The allocation-free contract is enforced by the `core` determinism
checkstyle overlay and by `scripts/jmh-regression.py`, which gates a > 10%
regression on the AverageTime mean (SampleTime means and tail percentiles are
advisory: they come from a noisy one-second histogram run) against
`config/jmh-baseline.json` and, with `--gc`, asserts a zero
`gc.alloc.rate.norm` per op on the hot-path benchmarks. The timing gate is
strict only on a controlled rig (local pre-commit) where the baseline and the
run share one environment; on a shared CI runner the absolute ns/op drifts
with host hardware and JDK build, so CI runs `--advisory-timing` and the
timing figures there are reported but never block the build. The `--gc`
zero-allocation contract is deterministic and machine-independent, so it stays
strict everywhere.

## Read replica

Eventually consistent reads. There is no latency contract; the replica must
never block its single poll thread (and thus query serving) for more than
`snapshotPollIntervalMs` / `archiveMessageTimeoutMs` per cycle, and a stalled
source must fail over within `livenessTimeoutMs`.

## Journaling

The domain-event journal never drops an event; the producer idles while the
ring is full. The journaler must drain fast enough that a full ring is the
exception, not the steady state (`JOURNAL_BACKPRESSURE` should stay near zero).
