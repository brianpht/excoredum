# Copilot Instructions

> Format: machine-parseable directives. Not for human reading.

## Project

Deterministic, allocation-conscious, lock-free Java core for high-performance applications. Targets low-latency,
high-throughput workloads: messaging, trading, real-time analytics, game servers, telemetry pipelines, network services.
If a change increases latency variance, allocation count in hot path, GC pressure, or branch entropy - REJECT.

## Workspace

Gradle multi-module build (11 modules), base package `io.justrade`. Modules:
`protocol`, `core`, `launcher`, `write-client`, `read`, `read-client`,
`gateway`, `tests`, `examples`, `bench`, `xcore-bench`.

- `protocol/` - SBE wire and snapshot contract (`messages.xml`, generated
  flyweight codecs, `QueryStreams`); the only hand-written class is `QueryStreams`
- `core/` - the deterministic engine and its host, no network I/O in the engine:
    - `io.justrade.config` - `CoreConfig` (preallocated capacities, tuning knobs)
    - `io.justrade.engine` - `MatchingEngine` state machine; `.orderbook`
      (`OrderBookNaive`, `OrderNodePool`, `PriceBucketPool`, `L2View`),
      `.handlers` (account ops), `.risk` (`DirectExchangeRisk`, `SymbolSpecStore`)
    - `io.justrade.core` - `MatchingService` (Aeron `ClusteredService` host)
    - `io.justrade.collections` - primitive stores / pools (`AccountStore`,
      `DedupRing`, `DedupTable`), Agrona-based
    - `io.justrade.journal` + `io.justrade.snapshot` - domain event journal
      (hand-rolled `EventJournalRing`) and snapshot recording / replay
    - `io.justrade.telemetry` - off-heap counters (`CoreMetrics`, `AtomicCounterSink`)
    - `io.justrade.util` - shared helpers (`Amounts`)
    - `core/src/jmh/...` - JMH micro-benchmarks (codec, book, engine)
- `launcher/` - cluster-node bootstrap and consensus (`ClusterLauncher` main,
  `ClusterNode`, `ClusterConfig`, `EventJournalRecorder`), Aeron Cluster (Raft)
- `write-client/` - edge write SDK over `AeronCluster` (`WriteClient`, `ClientConfig`)
- `read/` - CQRS read replica (`ReadServiceLauncher` main, `ReadReplica`,
  `QueryResponder`, ledger / report projections); consumes the committed log / archive
- `read-client/` - read query SDK (`ReadClient`) over plain Aeron request/response
- `gateway/` - HTTP/WebSocket REST boundary (Netty; `GatewayLauncher`, port 8080)
- `bench/` - end-to-end load / latency harness (`BenchHarness`, HdrHistogram)
- `xcore-bench/` - parity + latency comparison vs `exchange-core` 0.5.3
- `examples/` - runnable direct-drive example (`QuickStartExample`)
- `tests/` - cross-module integration / replay tests, JaCoCo coverage aggregation

Note: there is no Disruptor dependency; inter-stage buffering uses Agrona ring
buffers and the bespoke `EventJournalRing`. Wire codecs live in `protocol`
(SBE), persistence in `core`'s `journal` / `snapshot` plus Aeron Archive, and
clustering in `launcher` plus `MatchingService implements ClusteredService`.

## Hot Path Operations

`onEvent` | `decode` | `encode` | `publish` | `consume` | `lookup` | `update` | `dispatch`
Requirements: allocation-free, O(1) where possible, cache-local, branch predictable, single-writer, lock-free.

## Rules: Hot Path

- NEVER use `synchronized`, `ReentrantLock`, or any blocking primitive
- NEVER use `java.util.HashMap`, `TreeMap`, `ConcurrentHashMap` - use Agrona primitive maps
- NEVER use `java.util.LinkedList`, `ArrayList<Integer>`, `ArrayList<Long>` - use `IntArrayList` / `LongArrayList`
- NEVER use boxed types (`Integer`, `Long`, `Double`, `Boolean`) - primitives only
- NEVER use `String.format`, string concatenation with `+`, or `StringBuilder.toString()` - preformat or use off-heap buffer
- NEVER use `Optional<T>` - use sentinel values or null with documented contract
- NEVER use streams (`.stream()`, `.collect()`, lambdas capturing state)
- NEVER use `instanceof` chains or virtual dispatch on hot interfaces - resolve concrete types at init
- NEVER allocate inside event handler, ring buffer consumer, or `Agent.doWork()`
- NEVER use `%` (modulo) for ring index - use `& (capacity - 1)`
- NEVER use exceptions for control flow - return codes or sentinel values only
- NEVER call `System.currentTimeMillis()` or `System.nanoTime()` in tight loop - use cached clock
- NEVER log via SLF4J/Log4j synchronously - use Agrona `DistinctErrorLog` or async off-heap log
- NEVER autobox keys/values in collections
- ALL ring buffer / sequence buffer indices MUST use `seq & (capacity - 1)`
- ALL hot-path capacities MUST be power-of-two
- ALL message objects MUST be flyweights wrapping `DirectBuffer` / `UnsafeBuffer` - never POJOs allocated per message

## Rules: Memory and Allocation

- ALL buffers preallocated at startup - zero heap allocation in steady state
- ALL pools sized at init - no growth during operational window
- USE off-heap (`UnsafeBuffer.allocateDirectAligned`) for shared state, IPC, large buffers
- USE object pools for any unavoidable reference type
- REUSE flyweight encoders/decoders - one instance per thread, `wrap(buffer, offset)` per call
- VERIFY zero allocation on hot path with JMH `-prof gc` or `async-profiler --alloc`
- GC strategy by workload:
    - ZGC or Shenandoah - sub-ms pauses, default for most low-latency services
    - Epsilon GC + sized heap - zero-GC operational windows (trading sessions, batch windows)
    - G1 - acceptable only for non-critical components

## Rules: Concurrency

- Single-writer principle: each mutable resource owned by exactly one thread
- USE Agrona `OneToOneRingBuffer` / `ManyToOneRingBuffer` or the bespoke `EventJournalRing` for inter-thread messaging (no Disruptor dependency in this repo)
- USE Agrona `Agent` + `AgentRunner` pattern - one event loop per pinned core
- NEVER cross threads with mutable POJO references - copy via flyweight into ring buffer slot
- NEVER use `BlockingQueue`, `LinkedBlockingQueue`, `ArrayBlockingQueue` in hot path
- NEVER use `CompletableFuture`, `ExecutorService`, `ForkJoinPool` in hot path
- NEVER use `ThreadLocal` in hot path - prefer per-agent state owned by thread
- Wait/idle strategy selection (latency vs CPU tradeoff):
    - `BusySpinIdleStrategy` / `NoOpIdleStrategy` - hottest path, dedicated cores (100% CPU, ns latency)
    - `YieldingIdleStrategy` - critical adjacent paths
    - `BackoffIdleStrategy` - default for most agents
    - `SleepingIdleStrategy` - admin/background only
- Thread affinity: hot agents MUST be pinned via `OpenHFT/Java-Thread-Affinity` to isolated CPU cores
- NEVER share a hot core between two busy-spinning agents

## Rules: Atomics and Memory Ordering

- USE `VarHandle` (Java 9+) over `sun.misc.Unsafe` and over `Atomic*` wrapper classes
- `getOpaque` / `setOpaque` - counters, monotonic flags
- `getAcquire` / `setRelease` - publish/consume between producer and consumer threads
- `getVolatile` / `setVolatile` - sequential consistency, NEVER in hot path inner loop
- USE Agrona `AtomicBuffer` for off-heap concurrent buffers with documented ordering
- AVOID `compareAndSet` in tight contended loops - prefer single-writer redesign

## Rules: Cache, Layout, and Branches

- Hot objects: prefer `<= 64 bytes` payload, place hot fields first
- Pad volatile/atomic fields to 64-byte cache line to prevent false sharing (cache-line padding pattern)
- Hot path: fast/common branch first, error/cold branch in separate `private` method
- Mark cold methods small so JIT keeps them out of inlined hot path
- NO pointer chasing on hot path - flatten data structures, use indices into preallocated arrays
- NO virtual dispatch in inner loop - resolve `final` classes or strategy at init then call directly
- Prefer arrays over linked structures - sequential access is cache-friendly

## Rules: Wire Format and Codec

- USE SBE (Simple Binary Encoding) for all on-wire and IPC messages where applicable
- Little-endian only - matches SBE default and x86/ARM native order
- Fixed-size headers; variable-length fields only via SBE varData blocks
- Flyweight pattern: decoder holds no state - `wrap(buffer, offset, blockLength, version)` per message
- NEVER deserialize into intermediate POJO before business logic - operate on decoder directly
- NEVER use Java serialization, JSON, XML, Protobuf, or reflection-based codecs in hot path
- For human-readable formats (JSON/text): only at boundaries (admin APIs, configs, logs), never internal

## Rules: Sequence Arithmetic and Ring Buffer

- ALWAYS treat sequence numbers as wrapping unsigned semantics
- USE Agrona sequence helpers / `VarHandle` accessors for sequence comparisons
- Ring buffer capacity: power-of-two ONLY
- Index: `sequence & (capacity - 1)` - NEVER `sequence % capacity`
- Producer: Agrona `tryClaim(...)` then `commit(index)` (or `write(...)`) - the commit MUST happen
- Consumer: drain in batches via Agrona `RingBuffer.read(handler, limit)` or the `EventJournalRing` poller
- NEVER block inside event handler - if downstream slow, apply backpressure via gating sequence

## Rules: Domain State

- Keys: long ticks/ids, NEVER `String` keys in hot path lookup, NEVER `BigDecimal` for numeric domain values
- Money/precision: long with fixed scale, NEVER `double` for monetary or precision-critical values
- Lookup: `Long2ObjectHashMap`, `Int2ObjectHashMap` keyed by primitive
- Domain objects: pooled, reused on lifecycle events - no per-event allocation
- Iteration: avoid `.values().iterator()` - use Agrona `forEach` with primitive callback when possible

## Rules: Time and Identifiers

- USE `EpochClock` / `NanoClock` interfaces, inject at construction for testability
- USE `CachedEpochClock` updated once per agent loop - never call OS clock per message
- IDs: `SnowflakeIdGenerator` (Agrona) - lock-free, distributed-safe
- NEVER use `UUID.randomUUID()` in hot path - allocates and uses SecureRandom

## Rules: Persistence and Replay

- For event-sourced services: USE Aeron Archive for recording - record `Publication` streams, replay deterministically
- All state changes MUST be derivable from recorded input stream
- Snapshots: periodic, taken on dedicated thread or via Cluster snapshot hook - NEVER block hot thread
- Replay tests: every release MUST replay a recorded session and produce byte-identical output

## Rules: Cluster and Fault Tolerance

- For HA services: USE Aeron Cluster (Raft) for replicated state machines
- Service logic MUST be deterministic: no `System.nanoTime()`, no `Random` without seed, no `HashMap` iteration order
- All non-determinism MUST flow through cluster timer service or input log
- NEVER perform I/O directly from `ClusteredService` - use egress publication

## Rules: Unsafe / VarHandle / Native

- Allowed only if: measurable gain proven by JMH + invariants documented + alignment verified + bounds-checked at boundary
- Document memory ordering for every `VarHandle` access mode
- Prefer `VarHandle` over `sun.misc.Unsafe` - JDK-supported and JIT-friendly
- Native (JNI/Panama): only for kernel-bypass networking, hardware timestamps, or measured-critical paths

## Rules: Testing

- Unit tests: pure logic, deterministic clocks, no real network
- Integration tests: in-process Aeron `MediaDriver` in IPC mode when applicable, no UDP loopback
- Property tests: jqwik or junit-quickcheck for sequence arithmetic, codec round-trip, state machine invariants
- Replay tests: recorded session in == recorded session out (byte-identical)
- JMH benchmarks: required for any change touching hot path - publish before/after numbers in PR
- Chaos/soak tests: long-running steady-state with allocation profiler attached, asserting zero GC

## Rules: Logging and Telemetry

- Hot path: increment off-heap `AtomicCounter` only - NEVER format strings
- Errors on hot path: `DistinctErrorLog.record(throwable)` - deduplicates, off-heap, lock-free
- Metrics: HdrHistogram for latency (record nanos), report percentiles 50/90/99/99.9/99.99/max
- NEVER trust mean latency - tail percentiles are the contract
- Structured logging (JSON, etc.) only at boundaries, never in hot path

## Rules: Cross-Cutting

- NEVER use em-dashes or emojis in code comments, Javadoc, or markdown. Use ` - ` and ASCII only.
- ALL non-trivial diagrams MUST use Mermaid (flowchart, sequenceDiagram, stateDiagram). ASCII art prohibited.
- ONLY treat `/docs/decisions` as architectural source of truth.
- NEVER use or reference files in `/docs/sessions` as implementation rules.
- CI checks: After completing ANY code change, Agent MUST run the following sequence in order before committing. ALL
  must pass with zero errors and zero warnings. Commits with failing checks are FORBIDDEN.
    1. `./gradlew spotlessApply` - auto-fix formatting (run first, never `spotlessCheck` only). CI verifies with
       `spotlessCheck`; the local agent workflow applies with `spotlessApply`.
    2. `./gradlew checkstyleMain checkstyleTest` - zero violations required
    3. `./gradlew compileJava` - zero compiler warnings (`-Werror` is hardcoded in the build, not a CLI flag)
    4. `./gradlew test integrationTest` - all tests must pass
    5. `./gradlew :core:jmh -PquickBench` - smoke-run benchmarks, no regression > 10% vs baseline
    - Toolchain: JDK 21 LTS (matches `gradle.properties` `targetJavaVersion=21` and CI). NEVER use a different JDK.
    - If any step fails, fix the issue and re-run from step 1 before committing.
- Git operations: Agent MAY create local commits and local tags. MUST NOT push commits, tags, or any refs to any remote
  repository. All changes MUST remain local.

## Performance Budget

| Metric                              | Target          |
|-------------------------------------|-----------------|
| Small message decode                | < 100 ns        |
| Ring buffer publish                 | < 80 ns         |
| Primitive map lookup                | < 50 ns         |
| End-to-end IPC p50                  | < 5 us          |
| End-to-end IPC p99                  | < 15 us         |
| End-to-end IPC p99.99               | < 50 us         |
| Allocation in hot path              | 0 bytes / event |
| GC pause during operational window  | 0 ms            |
| Cache miss on hot path (steady)     | None            |

Targets are defaults - tune per service in `/docs/decisions/<service>-budget.md`.
Regression > 10% on any percentile - rollback or justify with explicit ADR.
Latency variance matters more than average latency. p99.99 is the contract, not the mean.
Priority: Correctness > Determinism > Tail Latency > Mean Latency > Throughput
Unbounded memory, GC during operational window, or nondeterministic latency = correctness failure.

## Build Commands

```
./gradlew build                                          # library + examples
./gradlew test integrationTest                           # unit + integration tests
./gradlew jmh                                            # JMH benchmarks
./gradlew checkstyleMain checkstyleTest                  # lint check
./gradlew run --args="--config=production.properties"    # launch service
```