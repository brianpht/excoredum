# Benchmarking Against exchange-core

The `exc-xcore-bench` module measures excoredum against the upstream
[exchange-core](https://github.com/exchange-core/exchange-core) 0.5.3 - the
engine whose matching semantics excoredum ports. It exists to answer one
question: for the same matching work, where do the two stand, and what does the
Aeron Cluster wrapper cost on top?

## What is compared

Four layers, each a separate mode of the same CLI:

| Mode     | excoredum side                                  | exchange-core side                          |
|----------|-------------------------------------------------|---------------------------------------------|
| `book`   | `OrderBookNaive`                                 | `OrderBookNaiveImpl` and `OrderBookDirectImpl` |
| `engine` | `MatchingEngine.process` (single thread: SBE decode, dedup, symbol / user checks, risk, matching, dedup store) | `ExchangeCore` disruptor pipeline (grouping, risk, matching, risk release, result future) |
| `e2e`    | In-process single-node Aeron Cluster driven by `ExcClient` (consensus + archive on the path) | Same disruptor pipeline (no consensus, no replication) |
| (all modes) | Closed-loop ops/s reported with the latency tables | same |

Only `book` is a strict apples-to-apples comparison: both sides run the
matching-only path synchronously on the caller thread. The other layers
compare different system shapes and must be read with that in mind.

## Fairness notes (read before quoting numbers)

- **Risk coverage differs by layer.** At the `book` layer neither side does
  risk checks. At `engine` and `e2e` both sides run spot risk (reserve on
  placement, settle at fill price, release on cancel). The excoredum engine
  path additionally runs per-client dedup; exchange-core has no equivalent at
  the matching layer.
- **Allocation asymmetry.** The excoredum engine path reuses SBE flyweights and
  allocates nothing in steady state (check with `-Pjmh.profilers=gc`). The
  exchange-core pipeline allocates an `ApiPlaceOrder` builder and a completion
  future per command; that cost is inherent to its public API.
- **Consensus is not free.** The `e2e` excoredum number pays for Raft log
  commit and archive recording on every command; exchange-core has no
  replication. The gap between `engine` and `e2e` on the excoredum side is the
  price of strong consistency, reported deliberately, not hidden.
- **No affinity pinning.** exchange-core's latency presets pin threads with
  OpenHFT affinity; this module runs plain threads, because affinity 3.x
  predates JDK 21 and pinning on shared hardware would distort the comparison.
  Both sides are equally unpinned.
- **Same JVM, same fork settings.** JMH runs every implementation inside the
  same benchmark class (`@Param`), so all three books share one JVM
  configuration and clock source.

## Workload

`book` mode replays a deterministic port of exchange-core's own
`TestOrdersGenerator` (from its test sources, Apache-2.0): a fill phase grows
the book to a target depth, then a benchmark phase issues the same moving
price / mix (GTC place, IOC, FOK-BUDGET, cancel, move, reduce) the upstream
tool generates. The port emits primitive arrays (`Workload`) instead of
`OrderCommand` lists so both engines iterate identical bytes.

Cross-validation is built into the replay: excoredum's book is the reference,
and each exchange-core implementation must produce identical trade / reject /
reduce counters and an identical full-depth L2 (prices and volumes per level).
Any divergence fails the run, so the comparison doubles as a parity test for
the port. `XcoreBenchSmokeTest` (tag `integration`) guards this on the CI gate.

## Dependency notes

- `exchange.core2:exchange-core:0.5.3` from Maven Central - the same version
  as the reference checkout under `tmp/exchange-core`.
- Gradle resolves agrona to this project's 2.2.1 (exchange-core declares
  1.4.1); exchange-core only uses `org.agrona.collections` types whose API is
  unchanged across that span.
- chronicle-wire is pinned to a JDK 21-compatible line (2.25ea) in this module
  only; the 2.19.1 exchange-core ships fails a reflective lookup of
  `sun.nio.ch.FileChannelImpl.unmap0` on modern JDKs. Chronicle is touched
  only by the binary symbol-registration command, not by matching.
- OpenHFT affinity arrives upgraded to the project's own 3.23.3 and is never
  used deliberately (see fairness notes).

## Running

```bash
# Matching-level replay with cross-validation (defaults mirror the upstream
# single-pair benchmark scale: 3M benchmark commands, 1K target orders, 1K users)
./gradlew :exc-xcore-bench:run --args="--mode=book --commands=3000000 --target-orders=1000 --iterations=3"

# Engine dispatch (single-thread full path) vs disruptor pipeline (1M measured ops)
./gradlew :exc-xcore-bench:run --args="--mode=engine --warmup=200000 --ops=1000000"

# Cluster end-to-end vs pipeline end-to-end (200K measured ops)
./gradlew :exc-xcore-bench:run --args="--mode=e2e --warmup=20000 --ops=200000"

# All three in one run
./gradlew :exc-xcore-bench:run --args="--mode=all"

# JMH comparison of the order books (same shapes as exc-core's OrderBookBenchmark
# plus a fresh-book replay chunk shaped after exchange-core's ITOrderBookBase)
./gradlew :exc-xcore-bench:jmh
./gradlew :exc-xcore-bench:jmh -PquickBench
./gradlew :exc-xcore-bench:jmh -Pjmh.profilers=gc
```

Sub-100K runs are smoke tests only: the book implementations and the pipeline
need the 3M-command scale to reach steady state, so small runs overstate the
excoredum book's advantage and understate the pipeline's steady-state
throughput. The harness reports through p99.9; the upstream README publishes
through 99.99 and worst, so deepest tails are not directly comparable.

Absolute numbers move with hardware; compare implementations on the same
machine, and prefer tail percentiles over means when drawing conclusions.

## Interpretation baseline

exchange-core's README publishes single-symbol risk + matching latencies
(excluding IPC and journaling) from ~0.5 us p50 at 125K ops/s up to ~1.5 us
p50 at 5M ops/s on tuned dual-Xeon hardware with frequency pinning and an
isolated socket. Numbers from this module on untuned shared hardware are not
directly comparable to that table; use the side-by-side ratios instead, and
treat the excoredum `engine` mode as the counterpart of their engine-only
figures.
