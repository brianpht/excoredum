# Performance budget

This is the budget source of truth for justrade. It overrides the defaults in
`.github/copilot-instructions.md`. A regression greater than 10% on any
percentile must be rolled back or justified with a new ADR.

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
regression on the mean (tail percentiles are advisory) against
`config/jmh-baseline.json` and, with `--gc`, asserts a zero
`gc.alloc.rate.norm` per op on the hot-path benchmarks.

## Read replica

Eventually consistent reads. There is no latency contract; the replica must
never block its single poll thread (and thus query serving) for more than
`snapshotPollIntervalMs` / `archiveMessageTimeoutMs` per cycle, and a stalled
source must fail over within `livenessTimeoutMs`.

## Journaling

The domain-event journal never drops an event; the producer idles while the
ring is full. The journaler must drain fast enough that a full ring is the
exception, not the steady state (`JOURNAL_BACKPRESSURE` should stay near zero).
