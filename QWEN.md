# QWEN.md - excoredum

> **CRITICAL:** At the start of every conversation, before any code changes, read
> `.github/copilot-instructions.md`. It contains the authoritative, machine-parseable
> coding directives for hot path, memory, concurrency, wire format, testing, and
> determinism. All code must comply with those rules regardless of what is summarized
> below.

## Project Overview

excoredum is a deterministic, replicated, in-memory **spot exchange matching
engine** in Java, built on **Aeron Cluster** (Raft). It ports the matching
semantics of [exchange-core](https://github.com/exchange-core/exchange-core)
(price-time priority, GTC / IOC / FOK-BUDGET orders, direct-exchange spot risk
with maker/taker fees) onto the Aeron ecosystem as a single `ClusteredService`.

Core invariants (these define correctness here):

- **Deterministic**: identical input logs produce byte-identical state and
  snapshots across nodes and reruns. No clock, no randomness, no unordered
  iteration in the engine; the only time source is the leader-assigned
  timestamp carried with each command.
- **Idempotent**: per-client dedup window keyed on `(clientId, clientSeq)`;
  every command is applied at most once across retries and leader failover.
- **Allocation-free hot path**: zero heap allocation during decode, match,
  settle, and ACK in steady state; resting orders are pooled.
- **Single-writer, lock-free**: one clustered-service thread owns all state.
- **Integer-only money math**: 64-bit fixed-scale amounts with
  overflow-checked arithmetic; no floating point in matching or settlement.

Business logic lives in `MatchingEngine` (no Aeron dependency, unit/replay
testable in isolation). `MatchingService` is the Aeron boundary: decode,
dedup, dispatch, ACK, egress events, journal emission, snapshots.

See `docs/ARCHITECTURE.md` for the component map, wire and snapshot formats,
data flows, determinism rules, and order-book semantics.

## Tech Stack

- **JDK 21 LTS** (toolchain-enforced; `targetJavaVersion=21` in
  `gradle.properties`). Linux required (Aeron media driver).
- Aeron 1.48 / Agrona 2.2 / SBE 1.35 (version catalog:
  `gradle/libs.versions.toml`).
- JUnit 5 (tagged suites), jqwik (property tests), JMH (benchmarks),
  HdrHistogram (latency tails).
- Root package: `com.exadbe`.

## Module Map

| Module         | Responsibility                                                            |
|----------------|---------------------------------------------------------------------------|
| `exc-protocol` | SBE schema (`src/main/resources/messages.xml`) + generated flyweight codecs. Wire contract only; never depends on `exc-core`. |
| `exc-core`     | Deterministic matching engine, order book, risk/fees, dedup, snapshot, telemetry, journal. This is the hot path. |
| `exc-launcher` | Aeron bootstrap: Media Driver, Archive, Consensus, Container, journaler agent. `main` class: `com.exadbe.launcher.ClusterLauncher`. |
| `exc-client`   | Client SDK (depends only on `exc-protocol`): leader-change handling, idempotent retry, correlation, egress events. |
| `exc-read`     | CQRS read replica and HA journal consumers (replay + dedup + failover), balance report generation, per-user order history ledger and market trade tape rebuilt from the log. |
| `exc-bench`    | End-to-end latency harness (in-process cluster + client, HdrHistogram).  |
| `exc-xcore-bench` | Comparative benchmarks vs exchange-core 0.5.3: replay parity, engine/pipeline latency, e2e, JMH. Exempt from determinism rules. |
| `exc-tests`    | Unit, property, integration, cluster, and fault suites + test fixtures.  |
| `exc-examples` | Runnable examples: in-process cluster driven through the client SDK (`QuickStartExample`). |

## Building and Running

```bash
./gradlew build                 # full build (check gate includes cluster+fault tests)
```

Aeron/Agrona need `--add-opens java.base/jdk.internal.misc=ALL-UNNAMED` and
`--add-opens java.base/sun.nio.ch=ALL-UNNAMED`; the root build applies these
to all Test tasks, and the launcher / read / bench application
configs set them automatically.

### Running services

```bash
# Single-node localhost cluster
./gradlew exc-launcher:run

# Node N of a multi-node cluster, preserving prior state across restarts
./gradlew exc-launcher:run -Dexc.nodeId=1 -Dexc.cleanStart=false

# From a deployment properties file (must define exc.clusterMembers)
./gradlew exc-launcher:run --args="--config=production.properties"

# CQRS read replica following a member's archive
./gradlew exc-read:run --args="--archive=aeron:udp?endpoint=localhost:20104"
```

### Testing

Suites are separated by JUnit 5 tags, all defined in `exc-tests`:

| Task              | Tag           | Notes                                            |
|-------------------|---------------|--------------------------------------------------|
| `test`            | (none of the below) | Unit + property tests. Excludes tagged suites.   |
| `integrationTest` | `integration` | In-process single-node Aeron cluster.            |
| `clusterTest`     | `cluster`     | Multi-node (election, warm restart, catch-up). In `check`. |
| `faultTest`       | `fault`       | Leader-kill / failover, exactly-once. In `check`. |
| `soakTest`        | `soak`        | Long-running soak/chaos. Opt-in only.            |

```bash
./gradlew test                    # unit + property only
./gradlew integrationTest         # in-process cluster integration
./gradlew clusterTest faultTest   # heavier multi-node / fault suites
./gradlew soakTest                # opt-in soak
```

Note: `check` (and therefore `build`) depends on `integrationTest`,
`clusterTest`, and `faultTest`.

### Benchmarks

```bash
./gradlew exc-core:jmh                    # full JMH run (JSON results)
./gradlew exc-core:jmh -PquickBench       # fast smoke run (CI gate)
./gradlew exc-core:jmh -Pjmh.profilers=gc # attach GC/allocation profiler
./gradlew exc-bench:run --args="--warmup=5000 --ops=20000"  # end-to-end RT latency
```

### Pre-commit gate (must pass in this order)

1. `./gradlew spotlessApply` - auto-format (Palantir Java Format). Run
   `spotlessApply`, not just `spotlessCheck`; CI verifies with the latter.
2. `./gradlew checkstyleMain checkstyleTest` - zero violations,
   `maxWarnings = 0`.
3. `./gradlew compileJava` - `-Werror` is hardcoded in the build (not a CLI
   flag); zero warnings required.
4. `./gradlew test integrationTest` - all green.
5. `./gradlew exc-core:jmh -PquickBench` - no regression > 10% vs baseline.

If any step fails, fix and re-run from step 1 before committing.

### Regenerating SBE codecs

Wire codecs are generated, not hand-written:

```bash
./gradlew exc-protocol:generateSbe
```

Schema: `exc-protocol/src/main/resources/messages.xml`. Evolve it via
optional fields / sinceVersion for backward compatibility. Generated sources
land in `exc-protocol/build/generated-src/sbe` and are excluded from lint and
formatting gates.

## Development Conventions

### Hot-path rules (enforced; see `.github/copilot-instructions.md`)

Changes that increase latency variance, hot-path allocation, GC pressure, or
branch entropy are rejected. On the hot path:

- No locks, no `synchronized`, no blocking primitives, no `CompletableFuture`
  / executors.
- No boxed types, no `java.util` hash/linked collections - use Agrona
  primitive maps and arrays.
- No streams, no capturing lambdas, no `Optional`, no string formatting, no
  exceptions for control flow (return codes / sentinels only).
- Ring indexing: `seq & (capacity - 1)`; all ring capacities power-of-two.
- Messages are flyweights wrapping `DirectBuffer` / `UnsafeBuffer`; reuse
  encoders/decoders via `wrap(...)`, never allocate per message.
- All buffers/pools preallocated at startup; no growth at runtime. Capacities
  live in `CoreConfig` (validated, power-of-two where they index a ring).
- Determinism: no `System.currentTimeMillis()` / `nanoTime()`, no `Random`,
  no unordered iteration in engine code. `exc-core` has its own checkstyle
  config (`exc-core/config/checkstyle/determinism.xml`) that bans clocks,
  randomness, `java.util` hash / linked collections, locks, blocking queues,
  executors, streams, `Optional`, `BigDecimal`, and `String.format` in main
  sources.
- Telemetry on the hot path: counter increments only; errors via off-heap
  counters / distinct error log, never formatted strings.

### Style and tooling

- Formatting: Spotless with Palantir Java Format, unused imports removed,
  trailing whitespace trimmed, files end with a newline. Run `spotlessApply`.
- Checkstyle baseline: `config/checkstyle/checkstyle.xml` (all modules);
  determinism overlay for `exc-core`.
- Compile: UTF-8, `-Xlint:all -Werror` for production sources. Test, JMH, and
  generated sources drop `-Werror`.
- ErrorProne + NullAway are opt-in via `-PwithErrorProne` (report-only until
  triaged clean).
- ASCII only: no em-dashes or emojis in code comments, Javadoc, or markdown
  (use ` - `). Diagrams must be Mermaid, not ASCII art.

### Testing practices

- Unit tests: pure logic, deterministic clocks, no real network.
- Integration tests: in-process Aeron Media Driver / cluster, tagged
  `integration`.
- Property tests (jqwik): replay determinism, codec round-trips, state
  machine invariants.
- Replay/determinism tests: recorded input must produce byte-identical output.
- Any change touching the hot path needs JMH before/after numbers.
- JaCoCo coverage is aggregated in `exc-tests` (its suites exercise
  `exc-core`, `exc-client`, `exc-launcher`, `exc-read`).

### Performance budget (defaults; p99.99 is the contract, not the mean)

| Metric                     | Target          |
|----------------------------|-----------------|
| Small message decode       | < 100 ns        |
| Ring buffer publish        | < 80 ns         |
| Primitive map lookup       | < 50 ns         |
| End-to-end IPC p99.99      | < 50 us         |
| Hot-path allocation        | 0 bytes/event   |

Priority: Correctness > Determinism > Tail Latency > Mean Latency >
Throughput. Regression > 10% on any percentile: roll back or justify with an
ADR.

### Git

Local commits and tags are fine. **Never push** commits, tags, or refs to any
remote.
