# core

The deterministic matching engine: the heart of justrade and its hot path. Given
a decoded command and a leader timestamp, it applies the command to the order
book, runs risk and settlement, and produces trades, reduces, and rejects, all
without allocating, locking, or reading a clock.

## Responsibility

- Price-time-priority order book with GTC, IOC, and FOK-BUDGET orders and
  PLACE / CANCEL / MOVE / REDUCE operations.
- Direct-exchange (spot) risk: fund reservation, settlement, maker/taker fees,
  and value conservation.
- Per-client dedup for exactly-once (idempotent) command processing.
- Deterministic snapshots and the event journal feed.
- Off-heap telemetry counters.

## Key classes

- [MatchingEngine.java](src/main/java/io/justrade/engine/MatchingEngine.java) -
  command dispatch and the matching loop.
- [MatchingService.java](src/main/java/io/justrade/core/MatchingService.java) -
  the `ClusteredService` integration (decode, process, encode egress).
- [orderbook/OrderBookNaive.java](src/main/java/io/justrade/engine/orderbook/OrderBookNaive.java) -
  the two-sided book (parity reference vs exchange-core).
- [risk/DirectExchangeRisk.java](src/main/java/io/justrade/engine/risk/DirectExchangeRisk.java) -
  risk checks, reserves, settlement, and fees.
- [risk/SymbolSpec.java](src/main/java/io/justrade/engine/risk/SymbolSpec.java) -
  per-symbol currencies, fees, and limits.
- [collections/](src/main/java/io/justrade/collections/) - account store, dedup
  table, and other primitive-backed ledger collections.

## Hot path rules

This module is subject to the strictest rules: no locks, no boxing, no
`HashMap`, no streams, no `String.format`, no per-event allocation, no clock
calls, power-of-two capacities with `& (capacity - 1)` indexing. A Checkstyle
determinism overlay enforces them. See [../CONTRIBUTING.md](../CONTRIBUTING.md#hot-path-rules)
and `.github/copilot-instructions.md`.

## Using the engine directly

The engine has no Aeron dependency and can be driven from a decoded command:

```java
MatchingEngine engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
CommandOutcome outcome = new CommandOutcome();
boolean duplicate = engine.process(commandDecoder, /* leader timestamp */ 1_000L, outcome);
```

## Benchmarks

JMH sources live under `src/jmh/`. Run the quick smoke set:

```bash
./gradlew :core:jmh -PquickBench
```

## Related

- Matching internals: [../docs/concepts/order-book-matching.md](../docs/concepts/order-book-matching.md).
- Risk and fees: [../docs/concepts/risk-and-fees.md](../docs/concepts/risk-and-fees.md).
- Determinism: [../docs/concepts/determinism-and-consensus.md](../docs/concepts/determinism-and-consensus.md).
- Design: [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md).
