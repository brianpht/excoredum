# Codebase Audit - 2026-08-22

> Full-repo audit of excoredum: invariant compliance (hot-path / determinism /
> memory / concurrency rules from `.github/copilot-instructions.md`),
> correctness of matching / risk / settlement / dedup / snapshot / journal,
> read-replica robustness, and test-coverage gaps. Every finding below was
> located by static scan or deep read and verified against the current working
> tree at commit `6238062`.

## Method

1. Static scans of all `src/main/java` sources for forbidden constructs
   (locks, clocks, randomness, `java.util` collections, boxing, streams,
   `Optional`, `String.format`, exceptions for control flow, `%` ring
   indexing, blocking primitives, console logging).
2. Four deep-dive passes: `exc-core` (correctness + invariants),
   `exc-read` + `exc-read-client` (CQRS correctness, failover, robustness),
   `exc-launcher` + `exc-write-client` + `exc-protocol` + build/config/docker,
   and a test-coverage gap analysis across all 43 test files.
3. Manual verification of every P0/P1 finding against the source.

## Executive summary

| Severity | Count | Summary |
|----------|-------|---------|
| P0       | 3     | Money minting via negative size; unchecked money-math overflow; read-replica ledger orphaned after snapshot bootstrap |
| P1       | 10    | MOVE fee-floor bypass; dedup sentinel break; silent journal drops without telemetry; sentinel-collision cluster crash; client dedup collision across incarnations; unbounded client busy-spin; replica wedge on stalled snapshot; HA journal consumer without liveness; zero unit coverage of the dedup ring |
| P2       | 19    | Validation gaps, checksum coverage, uint16 event counters, query-protocol hardening, docker pinning, JaCoCo aggregation, coverage gaps |
| P3       | 16    | Documentation, minor robustness, cold-path allocations, config hygiene |

**Invariant compliance overall is strong.** No locks, no clocks, no
randomness, no `java.util` collections, no boxing, no streams, no `Optional`,
no `String.format` anywhere in `exc-core` main sources; snapshot iteration is
explicitly sorted; pools are fixed with instrumented cold-path fallbacks. The
critical findings are concentrated in **input validation and integer overflow
discipline** (invariant #4 is violated in the risk/settle path) and in the
**read replica's snapshot-bootstrap orchestration**, not in the concurrency
model.

### Product decisions recorded during the audit

- **Self-trade**: allowed (matches upstream exchange-core; no uid comparison
  in `OrderBookNaive` matching). Documented here and in ARCHITECTURE.md; a
  conservation test covers the self-trade case.
- **Journal ring full**: the producer **blocks with idle** until the ring
  drains; events are never dropped. This supersedes the earlier "count and
  drop" description in ARCHITECTURE.md.
- **Fix scope of this round**: all P0 and P1 findings. P2/P3 remain as the
  backlog listed below.

### Upgrade note

The P0/P1 fixes change state-machine behavior (new reject paths, journal
backpressure) without changing the wire or snapshot format. Mixed-version
clusters would diverge, so **all nodes and read replicas must be upgraded
together**; restart from snapshot + log remains compatible.

### Resolution notes (this round)

- All P0 and P1 findings below are fixed; each fix is covered by a new or
  extended test (see the additions to the ARCHITECTURE.md test-coverage
  table).
- Baseline observation: before the fixes, the tree at commit `6238062`
  already failed `clusterTest` - `ReadReplicaCheckpointClusterTest` timed
  out ("replica never reached the expected state"). The failure was a
  symptom of F-03 (orphaned ledger after snapshot bootstrap); the test
  passes with the FIX-3 orchestration in place.
- Behavior worth knowing (documented, not a defect): a duplicate command
  replays the cached result verbatim, so the `CommandResult` carries the
  ORIGINAL submission's `commandId` - a re-sent command correlates under its
  first id, not the retry's.
- F-14 (ADD_SYMBOL validation) was pulled partially into this round as a
  prerequisite of F-02: the overflow bounds only hold with positive scale
  factors and non-negative fees (maker <= taker). Upper-bound caps remain
  backlog.
- F-18 (LedgerRebuilder image-close treated as caught-up) was fixed with
  F-03 because the stop-at-applied-position redesign subsumes it: the
  rebuild now stops exactly at the frozen applied position, and an image
  that closes early restarts the rebuild instead of swapping a partial
  ledger.
- Two P2 read-replica items were fixed in passing because the P0/P1 work
  exposed them. (1) A corrupt snapshot's logPosition is now remembered
  (`rejectedSnapshotPosition`) so a replica never reloads the same failing
  recording; without this, a fast snapshot-poll cycle could reset the
  replica in a loop and starve the rebuild. (2) The snapshot candidate scan
  skips the live consensus and journal recordings instead of replay-sniffing
  them (`SnapshotSubscriber.findNewestCandidate`), which previously stalled
  the poll thread; a stalled snapshot load now also aborts into a failover
  via `snapshotLoadTimeoutMs`. `SnapshotNegativeClusterTest` was reordered
  to wait for the corrupt rejection before growing the log, removing a race
  where the advance-only guard could skip the fabricated snapshot.

---

## P2/P3 resolution (2026-08-22)

This round addressed the P2 and P3 backlog. Summary of what changed and how:

- **F-12** - the snapshot checksum now folds the dedup record's uid/orderId/
  filledSize and their presence flags into `SnapshotManager.computeChecksum`.
  Covered by `SnapshotIntegrityTest.corruptedDedupFieldFailsInvariant`.
- **F-13** - `eventIndex`/`eventCount` are widened to uint32 via optional
  `sinceVersion=5` extension fields (`eventIndexExt` on TradeEvent / ReduceEvent /
  RejectEvent / JournalEvent, `eventCountExt` on CommandResult); encoders write
  both, decoders prefer the extension and fall back to the legacy uint16 for pre-v5
  frames. `JournalDedup` now keys on a `long` index. Schema bumped to version 5,
  `semanticVersion` 2.0 (F-42).
- **F-14** - `ADD_SYMBOL` now also rejects scale factors and fees above a
  documented upper bound (`AddSymbolHandler.MAX_SCALE_K` / `MAX_FEE`).
- **F-15** - dedup-window eviction is counted (`CoreMetrics.dedupEvictions`,
  mirrored off-heap) rather than silently ignored.
- **F-16** - `CoreConfig` validates all capacities (positive, power-of-two for
  `dedupWindow` and `journalSlotCount`) and gains a `builder()`; `DedupRing`
  rejects non-power-of-two capacities.
- **F-17** - snapshot load cross-checks the header's symbol/user/order/dedup-client
  counts against what actually decoded, and a balance for an unknown user raises a
  clean `IllegalStateException`; uid and orderId values that collide with the
  optional-field sentinels (`-1`, `Long.MIN_VALUE`) are rejected before caching.
- **F-18** - `probeIfDue` failover no longer NPEs and double-fails-over in one poll.
- **F-19** - ledger-rebuild failures are counted (`ReplicationHealth.rebuildFailures`).
- **F-20** - `listRecordings(0, N, ...)` caps replaced by paginated enumeration
  (`ArchiveRecordings`), so the newest recording is never missed.
- **F-21** - `awaitResolvedEndpoint` waits are bounded to 2 s (were 10 s); the
  consensus/journal recording sniffs were already fixed in the prior round.
- **F-22** - `QueryResponder` validates the response channel scheme, rejects a
  narrowing-negative stream id, and catches malformed-channel `addPublication`.
- **F-23** - both `QueryResponder` and `ReadClient` validate the message
  `schemaId`; unknown query types degrade to `UNSUPPORTED` (no NPE).
- **F-24** - async queries carry an overall budget (`messageTimeoutNs`) even with
  `maxRetries=0`, and a timed-out sync query releases its window slot.
- **F-25** - synchronous awaits use a small stack, so a nested sync query from a
  listener callback no longer corrupts the outer await.
- **F-26** - `ClientConfig.build()` rejects `maxRetries=0` with `dedupWindow=0`
  and validates the remaining knobs.
- **F-27** - reconnect uses the configured message timeout (not a hardcoded 5 s)
  and counts failures (`reconnectsFailures()`).
- **F-28** - `ClusterConfig` validates nodeId, host, members, and that the node is
  a member of its own members string.
- **F-29** - Docker base images pinned by digest; runtime runs as unprivileged
  user `excoredum`.
- **F-30** / **F-47** - `docs/decisions/` now exists with the performance budget
  and an ADR recording that hot agents are not pinned in-repo (deployment concern).
- **F-34** - `QueryResponder` reassembles fragments (`FragmentAssembler`).
- **F-35** - checkpoint write/load failures fall back to cold start and are
  counted (`ReplicationHealth.checkpointFailures`) instead of `printStackTrace`.
- **F-36** - `HaJournalConsumer` backs off between reconnect attempts and resumes
  from the last consumed position when reconnecting to the same recording.
- **F-38** - request ids never wrap through 0; `ReadClientConfig` validated.
- **F-39** - `ClusterConfig.ingressEndpoints(nodeCount, host)` multi-host variant.
- **F-40** - counters direct buffers are freed on close and on construction failure
  (allocated manually so the original buffer, not a slice, is freed).
- **F-41** - egress dispatch drops truncated frames; the NOP keepalive uses a
  reserved pool slot so a full window cannot starve session liveness.
- **F-43** - entrypoints take the first `hostname -i` address; healthchecks probe
  a pid file (`kill -0`) instead of depending on `pgrep`.
- **F-44** - exc-tests comments fixed (cluster/fault ARE in `check`); exc-protocol
  checkstyle scoped to hand-written `QueryStreams.java` only.
- **F-45** - JaCoCo report and a verification task merge all test tasks' exec data
  and set a floor.
- **F-46** - `.github/workflows/ci.yml` and `scripts/jmh-regression.py` added.

Not changed (documented, deliberate):

- **F-31** - duplicate `orderId` on PLACE still matches then rejects the remainder
  (exchange-core semantics, deterministic; changing it would break log-replay
  determinism).
- **F-32** - pool exhaustion still falls back to `new` (instrumented design).
- **F-37** - the read replica's replay loop still allocates `MarketTrade`/`Fill`
  records and uses `ArrayList.remove` on eviction; the read side is not the hot
  path and its records are returned to callers by reference.

Test additions: `InputValidationTest` (uid/orderId sentinels, scale/fee caps,
eviction counter), `CoreConfigTest`, `NegativeResultCodesTest`,
`SnapshotIntegrityTest.corruptedDedupFieldFailsInvariant`,
`EventCodecExtRoundTripTest`, plus the `InMemorySnapshot.corruptFirstDedupUid`
fixture.

Upgrade note: F-13 widens the wire format (schema v5) and F-14/F-17 add reject
paths, so all nodes, write clients, read replicas, and HA journal consumers must
be upgraded together. Pre-v5 journal recordings decode via the legacy uint16
index; new recordings carry the extension field. Snapshot and consensus-log
formats are unchanged.

## P0 findings (fixed this round)

### F-01 - PLACE accepts negative size / price: money minting

`exc-core/src/main/java/com/exadbe/engine/MatchingEngine.java:186-214`

`handlePlace` validates symbol, user, suspension, reserve price, and the ask
fee floor - but never `size > 0` or `price > 0`. A BID GTC with
`size = -1` produces a negative hold; `DirectExchangeRisk.reserve` then
executes `accounts.addToValue(uid, currency, -amount)`, which with a negative
`amount` **credits** the user's balance and returns `true`. The order then
rests with `remaining() == -1` (negative bucket volume, negative L2 sizes),
and a later contra-order fills `-1`, producing negative-size trades and
negative settlement credits. The same credit path is reachable with positive
inputs when `size * (reserveBidPrice * quoteScaleK + takerFee)` wraps
negative (see F-02). `CommandResultCode.INVALID_AMOUNT` exists in the schema
for exactly this and is produced nowhere.

Fix: reject `size <= 0` / `price <= 0` (and FOK-BUDGET budget <= 0) with
`INVALID_AMOUNT` before any state mutation; `reserve()` additionally refuses
negative amounts as defense in depth.

### F-02 - Money math is not overflow-checked in the risk/settle path

`exc-core/src/main/java/com/exadbe/engine/risk/DirectExchangeRisk.java:24-111`,
`exc-core/src/main/java/com/exadbe/engine/MatchingEngine.java:196,312,327`

Invariant #4 (integer-only, overflow-checked arithmetic) is only upheld in
`BalanceAdjustmentHandler` (the single use of `Amounts.addOverflows` in the
repo). All of these are unchecked `long` multiplies/adds:

- `bidHold` / `bidBudgetHold` / `askHold`
- `settleMaker` (both branches), `settleTakerBuy`, `settleTakerSell`
- the ask fee-floor check itself (`price * quoteScaleK < takerFee` - wrap
  flips the comparison either way)
- `sizePriceSum += size * price` accumulated across every maker swept by one
  command (aggregate overflow possible without any single oversized order)
- per-event release math in `releaseRejects` / `releaseReduces`

Fix: extend `Amounts` with checked multiply/add helpers (boolean-return, no
exceptions) and apply them throughout the risk/settle path, surfacing
`OVERFLOW` before state mutation; bound inputs at validation so the common
match loop stays branch-light.

### F-03 - Read replica: rebuilt ledger orphaned by the live subscriber

`exc-read/src/main/java/com/exadbe/read/ExcReadReplica.java:121-133,233`,
`exc-read/src/main/java/com/exadbe/read/LiveLogSubscriber.java:44`

`poll()` calls `ensureLiveLog()` before `pollLedgerRebuild()`.
`ensureLiveLog()` constructs the `LiveLogSubscriber` with the *current*
`this.ledger` (a `final` field captured at construction). After a snapshot
load, `pollLedgerRebuild()` swaps `this.ledger` for the rebuilt instance but
never restarts the live subscriber - which keeps applying every subsequent
command to the **old** ledger. From the swap onward, all ledger-backed
queries (`ORDER_HISTORY`, `ACTIVE_ORDERS`, `ORDER_BY_ID`, `USER_TRADES`,
`MARKET_TRADES`) serve the frozen rebuilt ledger, and checkpoints persist
`(engine at position P, ledger covering only up to swap-time)` - a warm
restart then loses the tail permanently. This fires on the normal path:
every replica that bootstraps from a cluster snapshot enters this state once
the rebuild completes.

Fix: hold the live log off while a rebuild is pending or running
(`ensureLiveLog` guard), and restart the live log at the swap so the new
subscriber binds the new ledger and resumes from `appliedPosition`.

---

## P1 findings (fixed this round)

### F-04 - MOVE bypasses the ask fee floor and price positivity

`exc-core/src/main/java/com/exadbe/engine/orderbook/OrderBookNaive.java:150-172`

The only price validation in `move` is the bid reserve cap. A resting ask can
be moved to a price whose proceeds are below the taker fee (the PLACE-path
`RISK_ASK_PRICE_LOWER_THAN_FEE` guard has no MOVE counterpart), and
`settleTakerSell` then credits a negative amount; `AccountStore.addToValue`
has no floor, so the quote balance goes negative with no reserve backing it.
`newPrice <= 0` is accepted on both sides.

### F-05 - clientSeq == EMPTY sentinel is wire-legal and breaks dedup

`exc-core/src/main/java/com/exadbe/collections/DedupRing.java:55-78`

`clientSeq` is `uint64` on the wire, so `0xFFFFFFFFFFFFFFFF` (= -1L =
`EMPTY`, the unoccupied-slot sentinel) is a legal client value. For that seq,
`contains` is always false (every submission applies fresh - idempotency
lost), and `put` writes the EMPTY sentinel into the slot, **erasing the dedup
record of whichever other sequence collides on `seq & mask`** - a retry of
that innocent sequence is then re-applied (double fill / double settlement).

### F-06 - Journal events silently dropped when the ring is full; zero telemetry

`exc-core/src/main/java/com/exadbe/journal/DomainEventJournal.java:44-50`,
`exc-core/src/main/java/com/exadbe/core/MatchingService.java:132`,
`exc-launcher/src/main/java/com/exadbe/launcher/ClusterNode.java:165`

The ring javadoc promises "the caller applies backpressure and never drops",
but `emit` increments a private `droppedEvents` counter and keeps offering
subsequent events (a transient full ring drops event `i` while accepting
`i+1` - a gap), `MatchingService` discards the `emit` return value, and
`droppedEvents()` is wired to nothing. Separately, the journaler agent's
error handler is `Throwable::printStackTrace`: the recorder throws
`IllegalStateException` on `CLOSED` / `MAX_POSITION_EXCEEDED`, the ring's
consumer position does not advance, the same batch is redelivered forever
with unbounded stderr spam, the ring fills, and the producer starts dropping.
Net effect: permanent journal loss with no observable signal, contradicting
"audit-ready by default".

Fix: producer blocks with idle until the ring drains (never drops), a
`journalBackpressure` counter is mirrored to the off-heap sink, the recorder
recovers from a closed publication instead of throwing, and its error path
counts via the off-heap counters manager.

### F-07 - Balance equal to Long.MIN_VALUE collides with the MISSING sentinel

`exc-core/src/main/java/com/exadbe/engine/handlers/BalanceAdjustmentHandler.java:25-33`,
`exc-core/src/main/java/com/exadbe/collections/AccountStore.java:20,81-83`

`addOverflows(0, Long.MIN_VALUE)` is false, so `BALANCE_ADJUSTMENT` with
`delta = Long.MIN_VALUE` on a zero balance stores the value into a
`Long2LongHashMap` whose `missingValue` is exactly `Long.MIN_VALUE` - Agrona
rejects storing the missing value with `IllegalArgumentException`, an
exception on the deterministic hot path that takes down every replica
identically. Even if stored, `balance()` would alias the sentinel to zero.

### F-08 - Write client: clientSeq restarts at 0 per JVM with a fixed clientId

`exc-write-client/src/main/java/com/exadbe/write/client/ExcClient.java:85`

The engine dedups on `(clientId, clientSeq)` and those records survive
restarts via snapshot. A restarted client with the same clientId re-sends
seqs 0, 1, 2, ... and the engine returns the OLD cached results without
applying the new commands. All in-repo callers use a fixed clientId; in-repo
e2e only passes because compose always clean-starts. The documented
warm-restart scenario (`-Dexc.cleanStart=false`) breaks.

Fix: `ClientConfig.initialClientSeq` lets a long-lived deployment advance the
sequence epoch per incarnation; documented in the builder javadoc.

### F-09 - Write client: offerUntilSent is an unbounded busy-spin

`exc-write-client/src/main/java/com/exadbe/write/client/ExcClient.java:626-649`

The loop exits only on success, `sessionLost`, or `CLOSED`. A publication
stuck in `NOT_CONNECTED` / `ADMIN_ACTION` / sustained backpressure spins one
core at 100% indefinitely and blocks `poll()` on the same thread, so
keepalives stop too. Fix: bounded by deadline; an unsent command becomes a
queued-unsent pending entry that `poll()` offers before any later command,
preserving submission order without blocking the caller.

### F-10 - Read replica: stalled snapshot load wedges the replica forever

`exc-read/src/main/java/com/exadbe/read/ExcReadReplica.java:129`,
`exc-read/src/main/java/com/exadbe/read/SnapshotSubscriber.java:133-141`

While a snapshot load is in flight, `ensureLiveLog()` refuses to run, and
`poll()` returns at the `liveLog == null` check before the liveness check
that drives `failover()`. `SnapshotSubscriber` completes only via
`loadComplete()`; a truncated recording whose header sniffed OK but whose body
never completes leaves the replica in `markStale` forever with no failover,
no live log, and no probe. Fix: a load deadline that aborts to failover.

### F-11 - HaJournalConsumer has no liveness detection

`exc-read/src/main/java/com/exadbe/read/HaJournalConsumer.java:72-79`

The only failover trigger is "had an image, then image count dropped to 0".
A source that stops delivering fragments while keeping the image open
(dead process holding the UDP publication, network blackhole) stalls it
forever; likewise when the image never appears after `startReplay`. Unlike
`ExcReadReplica`, there is no activity timer. Fix: last-activity tracking
with timeout cycling sources, mirroring the replica's probe.

### T-01 - DedupRing / DedupTable have zero unit coverage

Exactly-once and money correctness rest on `seq & (capacity - 1)` indexing,
eviction semantics, and per-client isolation in the dedup ring; the only
existing test is a single-client single-repeat case in
`MatchingEngineTest`. Added this round: ring/table unit tests plus property
tests for window wrap, eviction, multi-client isolation, and the EMPTY
sentinel.

---

## P2 backlog (not fixed this round)

| Id | Area | Finding |
|----|------|---------|
| F-12 | exc-core | Snapshot checksum omits dedup `uid`/`orderId`/`filledSize` and presence flags (`SnapshotManager.java:388-396`); corruption confined to those fields passes `verifyInvariant()` and becomes committed state |
| F-13 | exc-core | `eventIndex` / `eventCount` are uint16 but events per command are unbounded (`CommandOutcome` grows); a sweep of more than 65535 resting orders wraps `eventIndex` and `JournalDedup` rejects the rest as duplicates |
| F-14 | exc-core | `ADD_SYMBOL` stores the spec verbatim: negative fees act as subsidies, `scaleK <= 0` breaks hold math, `baseCurrency == quoteCurrency` collides reserves |
| F-15 | exc-core | Dedup window overwrite (eviction) is neither enforced nor counted by the engine; only the bundled SDK coordinates with the window |
| F-16 | exc-core | `CoreConfig` has no validation at all (no builder checks, no power-of-two enforcement); `DedupRing` does not validate capacity - latent until a properties/builder path exists |
| F-17 | exc-core | Snapshot load failure modes: NPE (not a clean state error) on a `BalanceRecord` whose uid has no `UserRecord`; header counts never cross-checked; optional-field sentinels ambiguous with legal values |
| F-18 | exc-read | `probeIfDue` failover causes an NPE and a double failover in the same `poll()` (`ExcReadReplica.java:137-138`): probe sets `liveLog = null`, the next line dereferences it, the catch calls `failover()` again - source advances two members per cycle |
| F-19 | exc-read | Ledger rebuild retry churn: each failed `LedgerRebuilder.start` allocates a whole engine + outcome with no backoff and no failure counter |
| F-20 | exc-read | `listRecordings(0, 100/200, ...)` caps miss the newest recording once a member accumulates more recordings - replica needlessly wipes state (`resetReplication`) or HA consumers pick stale recordings |
| F-21 | exc-read | PARTIALLY FIXED - Blocking busy-waits on the single poll thread: `awaitResolvedEndpoint` (10 s deadline) in four classes and 10 s per-recording sniff in `SnapshotSubscriber` freeze replication and queries, and can trigger spurious failover on resume. The consensus / journal recording sniffs and the unbounded snapshot load are fixed; the remaining `awaitResolvedEndpoint` waits are bounded by deadline |
| F-22 | exc-read | `QueryResponder` trusts client-supplied `responseChannel` / `responseStreamId`: open UDP redirector/amplifier, malformed channels throw out of `poll()`, unchecked uint32 narrowing, per-request channel churn evicts the 64-entry LRU |
| F-23 | exc-read / exc-read-client | Unknown `QueryType` (newer peer) returns `null` from the SBE enum and NPEs on both sides; neither side validates `schemaId`/`version` |
| F-24 | exc-read-client | `maxRetries = 0` means infinite retransmits (async has no overall budget); sync queries abandoned by `await()` timeout keep retransmitting and occupying window slots - a dead replica exhausts the window |
| F-25 | exc-read-client | Sync await slot is single-level: a synchronous query issued from a listener callback overwrites `syncAwaitId` and hangs to timeout, corrupting the outer await |
| F-26 | exc-write-client | `maxRetries = 0` + `dedupWindow = 0` permanently exhausts the in-flight window when a result never arrives; reject the combination at `ClientConfig.build()` |
| F-27 | exc-write-client | Reconnect hardcodes a 5 s message timeout (initial connect uses the configured 30 s); reconnect failures are swallowed with no counter |
| F-28 | exc-launcher | `ClusterConfig` performs no validation of the members string, nodeId membership, host, or computed ports; `exc.baseDir` defaults to a CWD-relative path |
| F-29 | docker | Base images `eclipse-temurin:21-jdk/jre` float unpinned (no digest); all containers run as root (no `USER`) |
| F-30 | build | `docs/decisions` is mandated by copilot-instructions as the ADR / budget source of truth but does not exist |

### P2 test-coverage backlog

- Price-time priority is only asserted indirectly (parity/replica tests) -
  add a direct FIFO-at-level unit + property test.
- Negative result-code paths untested: `INVALID_SYMBOL`,
  `UNSUPPORTED_COMMAND`, `MATCHING_REDUCE_FAILED_WRONG_SIZE`, wrong-uid
  cancel/move/reduce (security-relevant), engine-level `RESET`.
- Dedup survival across snapshot + warm restart is not functionally verified
  (`SnapshotWarmRestartIntegrationTest` switches clientId).
- No full mixed-stream rerun determinism test (cancel/move/reduce/IOC/FOK/
  trade sequences rerun and compared byte-for-byte); only balance-adjust and
  place-only streams are covered today.
- No standalone codec round-trip tests for `CommandEnvelope` /
  `CommandResult` / egress events / `JournalEvent` / snapshot records, and
  no schema-evolution (sinceVersion v2/v3/v4) decode tests.
- Cluster member catch-up (lagging follower restart) untested.

---

## P3 backlog (not fixed this round)

| Id | Area | Finding |
|----|------|---------|
| F-31 | exc-core | Duplicate `orderId` on PLACE sweeps the book before rejecting the remainder (documented in the schema, deterministic - but diverges from up-front rejection) |
| F-32 | exc-core | Pools and the event buffer fall back to `new` on exhaustion (instrumented counters; documented design) - strict reject-on-exhaustion is the alternative |
| F-33 | exc-read | `OrderLedger`'s `DUPLICATE` skip is a dead check (dedup hits return the cached original code, not `DUPLICATE`); the real protections are `eventCount == 0` + `ordersById` guard - misleading comment is a maintenance trap |
| F-34 | exc-read | `QueryResponder` request path has no `FragmentAssembler` (a >MTU `QueryRequest` with a long responseChannel would mis-decode) |
| F-35 | exc-read | Checkpoint `IOException` handled via `e.printStackTrace()` (invisible operationally); a corrupt checkpoint is fatal instead of falling back to cold start |
| F-36 | exc-read | `HaJournalConsumer` reconnect has no backoff and replays the full journal from position 0 on every failover (dedup absorbs it; O(history) bandwidth) |
| F-37 | exc-read | Steady-state allocation in the replay loop: full tape allocates a `MarketTrade` per trade; `Fill` records per fill; `ArrayList.remove` O(n) on eviction |
| F-38 | exc-read-client | `nextRequestId` wraps to 0 and collides with the `syncAwaitId == 0` sentinel after 2^63 submits; `ReadClientConfig.Builder.build()` performs no validation |
| F-39 | exc-launcher | `ingressEndpoints()` hardcodes localhost with no multi-host variant |
| F-40 | exc-launcher | `CountersManager` direct buffers are never released explicitly and leak on construction failure |
| F-41 | exc-write-client | Egress dispatch has no malformed-frame defense (truncated frame throws out of `pollEgress`); keepalive is skipped while the window is full |
| F-42 | exc-protocol | `semanticVersion` stuck at "1.0" across four schema revisions |
| F-43 | docker | `entrypoint-node.sh` uses `hostname -i` which can return multiple IPs (invalid channel URI); healthchecks depend on `pgrep` being present |
| F-44 | build | `exc-tests` comments claim cluster/fault are not wired into `check` (they are); QWEN.md's "-Werror dropped for tests" is inaccurate for most modules; `exc-protocol` disables checkstyle module-wide (also exempting hand-written `QueryStreams.java`) |
| F-45 | build | JaCoCo aggregation only merges `test.exec` + `integrationTest.exec`; cluster/fault/soak execution data is silently excluded; no coverage threshold |
| F-46 | build | No JMH baseline/regression comparison exists (QWEN.md mandates a 10% gate); zero-allocation is only observed when a human attaches `-Pjmh.profilers=gc`; no CI workflows exist (`.github/workflows` absent) |
| F-47 | build | `libs.affinity` declared in `exc-launcher` but unused; the "hot agents pinned via affinity" rule is unimplemented (`ThreadingMode.SHARED`, no pinning) - implement or document the deviation |

---

## Structural observations (informational)

- Only one property test exists in the repo (`EngineDeterminismTest`,
  balance-adjust only) despite the rules requiring jqwik for sequence
  arithmetic, codec round-trips, and state-machine invariants. This round
  adds property tests for the dedup ring and money-math boundaries.
- `ChaosSoakTest` asserts bounded GC collections, not zero hot-path
  allocation; strengthening it is left to the soak suite's owners.
- Determinism checkstyle (`exc-core/config/checkstyle/determinism.xml`)
  replaces (not overlays) the baseline for exc-core; it already contains
  every baseline rule plus the determinism bans. It does not ban
  `ArrayList` / `LinkedHashMap` / `HashSet` / `Executors` imports; exc-core
  main sources are empirically clean, but the ban list could be widened.
- Test flakiness posture is good: engine unit tests use no clocks/sleeps;
  cluster/integration tests use deadline-bounded polls with `@Timeout`.

## Verified correct (audited, no action)

- Price-time priority, GTC/IOC/FOK-BUDGET semantics, MOVE time-priority
  reset, partial-fill handling, pool reuse field-reset.
- Reserve/settle algebra (modulo the overflow fixes): bid reserve includes
  the taker fee, maker refunds, fee-account accrual, value conservation -
  hand-checked and property-tested.
- Dedup happy path: cached results replayed verbatim, `eventCount = 0` on
  duplicates, no egress/journal re-emission.
- Determinism: all state- and snapshot-reaching iterations explicitly
  sorted; the only time input is the leader-assigned timestamp;
  `cluster.time()` used solely for latency metrics.
- `EventJournalRing` SPSC mechanics, `JournalDedup` monotonic gate, egress
  and snapshot backpressure handling, write-client dedup-window expiry guard
  and bounded correlation map, replica position-monotonic failover,
  checkpoint atomicity, query buffer budgeting with `TRUNCATED` degradation.
