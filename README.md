# excoredum - Deterministic Spot Matching Engine

[![CI](https://github.com/brianpht/excoredum/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/brianpht/excoredum/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![JDK 21](https://img.shields.io/badge/JDK-21-blue.svg)](gradle.properties)
[![Gradle](https://img.shields.io/badge/Gradle-8.10.2-green.svg)](gradle/wrapper/gradle-wrapper.properties)
[![Platform: Linux](https://img.shields.io/badge/Platform-Linux-lightgrey.svg)](README.md)

A deterministic, replicated, in-memory spot exchange matching engine in Java,
built on Aeron Cluster: strong consistency, ultra-low latency, and an
allocation-free hot path for the order book and its balances.

## Why

Matching engines are correctness-critical. Price-time priority must be fair,
settlement must conserve value (including fees), every command must apply
exactly once even across retries and leader failover, and results must be
reproducible for audit and reconciliation. The hot path must stay fast and
predictable under load.

excoredum solves this as a single deterministic state machine replicated by
Aeron Cluster (Raft). The engine has no clock, no randomness, no floating
point, and no unordered iteration: identical input logs produce byte-identical
state and snapshots on every node and every rerun. Commands are idempotent by
design, and the steady-state hot path allocates nothing.

## Highlights

- **Deterministic and reproducible** - identical input logs produce
  byte-identical state, snapshots, and state hashes on every node, so replicas
  and archived logs can always be reconciled with each other.
- **Exactly-once, even across failover** - a per-client dedup window
  (`clientId`, `clientSeq`) makes every command idempotent: client retries, a
  leader change, or a killed leader can never double-apply a command.
- **Zero-allocation hot path** - decode, match, settle, and ACK allocate
  nothing in steady state; resting orders are pooled. The latency contract is
  the tail, not the mean: p99.99 governs.
- **Financially correct settlement** - integer-only 64-bit arithmetic with
  overflow checks, maker / taker fees, and value conservation across every
  trade including fees (a property test asserts taker + maker + fee is
  constant).
- **Audit-ready by default** - every committed trade / reduce / reject is
  written to a highly-available journal on the Aeron Archive, off the
  consensus thread: the producer blocks (never drops) until the journaler
  drains, and consumers dedup on `(logPosition, eventIndex)` for
  exactly-once delivery that survives a leader loss.
- **Reads without touching consensus** - a CQRS read replica follows a
  member's archive, rebuilds per-user order history and a market trade tape
  from the log, and answers queries over a plain Aeron protocol.

## Features

- Price-time priority order book: GTC, IOC, and FOK-BUDGET orders, plus
  PLACE / CANCEL / MOVE / REDUCE.
- Direct-exchange spot risk: funds reserved on placement, settled at the fill
  price, released on cancel / reduce; maker / taker fees accrue to a fee
  account (no margin).
- Account lifecycle: ADD_USER, BALANCE_ADJUSTMENT, ADD_SYMBOL, SUSPEND_USER /
  RESUME_USER, RESET, NOP.
- Native deterministic snapshots: engine state streamed to the Archive in
  sorted key order with an integrity checksum; warm restart replays only the
  remaining log.
- SBE wire format: fixed binary layout, no reflection, backward-compatible
  schema evolution via optional fields.
- Read-side SDK: balances, L2 book, user reports, order history, trade tape,
  and value-conservation totals over request/response streams, with idempotent
  retry and a bounded in-flight window.
- Off-heap telemetry: single-writer counters mirrored to a standalone
  `CountersManager`, readable from any thread without perturbing the hot path.

## Quick Start

Requires JDK 21 and Linux (Aeron media driver). The launcher, read, bench, and
example configurations set the required `--add-opens` JVM flags automatically.

```bash
# Runnable example: in-process single-node cluster, prints every egress event
./gradlew :exc-examples:run

# Single-node localhost cluster
./gradlew :exc-launcher:run

# Read replica following a member's archive (answers queries on port 44000)
./gradlew :exc-read:run --args="--archive=aeron:udp?endpoint=localhost:20104"
```

The engine has no Aeron dependency, so it can be driven directly from a decoded
command:

```java
MatchingEngine engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
CommandOutcome outcome = new CommandOutcome();
boolean duplicate = engine.process(commandDecoder, /* leader timestamp */ 1_000L, outcome);
```

Containerized end-to-end system test (3-node Raft cluster + read replica +
write load + read verify; exit 0 = all checks passed):

```bash
docker compose -f docker/docker-compose.yml up --build
```

## Performance

Indicative JMH numbers on x86_64 Linux, JDK 21 (steady state, zero allocation):

| Operation                      | Time    |
|--------------------------------|---------|
| Envelope decode / encode       | ~2 / ~3.7 ns |
| IOC match (1 fill, deep book)  | ~6.2 ns |
| Full dispatch (place + cancel) | ~84 ns  |
| Journal emit (4 events)        | ~41.6 ns |

Targets: decode < 100 ns, primitive-map lookup < 50 ns, end-to-end IPC p99.99
< 50 us, hot-path allocation 0 bytes. The `exc-xcore-bench` module benchmarks
against upstream exchange-core 0.5.3 (replay parity, engine and e2e latency,
JMH) - see [docs/BENCHMARKING-XCORE.md](docs/BENCHMARKING-XCORE.md).

## Modules

| Module              | Responsibility                                                   |
|---------------------|------------------------------------------------------------------|
| `exc-protocol`      | SBE schema and generated flyweight codecs (wire contract only)   |
| `exc-core`          | Deterministic engine: order book, risk, dedup, snapshot, journal, telemetry |
| `exc-launcher`      | Aeron bootstrap: media driver, archive, consensus, container     |
| `exc-write-client`  | Write-side SDK: leader-change handling, idempotent retry, egress events |
| `exc-read`          | CQRS read replica and HA journal consumers (order ledger, trade tape) |
| `exc-read-client`   | Read-side SDK over plain Aeron request/response streams          |
| `exc-bench`         | End-to-end latency harness (HdrHistogram tails)                  |
| `exc-xcore-bench`   | Comparative benchmarks vs exchange-core 0.5.3                    |
| `exc-examples`      | Runnable examples (in-process cluster + client SDK)              |
| `exc-tests`         | Unit, property, integration, cluster, and fault suites           |

Details - wire and snapshot formats, data flows, determinism rules, and
order-book semantics - live in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Testing

```bash
./gradlew test            # unit + property tests
./gradlew integrationTest # in-process single-node cluster
./gradlew clusterTest faultTest  # multi-node and fault-injection suites (in the default `check` gate)
```

## License

MIT License. See [LICENSE](LICENSE).

## Credits

Built on [Aeron](https://github.com/aeron-io/aeron),
[Agrona](https://github.com/aeron-io/agrona), and
[Simple Binary Encoding](https://github.com/aeron-io/simple-binary-encoding);
the matching model is a port of [exchange-core](https://github.com/exchange-core/exchange-core).
