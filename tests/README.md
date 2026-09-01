# tests

Shared test fixtures and the cross-module test suites, organized by JUnit 5 tags
so each tier can run independently in the build and in CI.

## Tiers

- Unit and property tests (no tag): pure logic with deterministic clocks and no
  real network. Property tests use jqwik for sequence arithmetic, codec
  round-trips, and settlement conservation.
- Integration (`@Tag("integration")`): in-process single-node cluster.
- Cluster (`@Tag("cluster")`): multi-node Raft with election, warm restart, and
  catch-up.
- Fault (`@Tag("fault")`): leader-kill and failover, verifying exactly-once.
- Soak: long-running steady-state runs with an allocation profiler attached.

## Run

```bash
./gradlew test                   # unit + property
./gradlew integrationTest        # in-process single-node cluster
./gradlew clusterTest faultTest  # multi-node and fault-injection suites
```

Cluster and fault suites run in the default `check` gate and in CI (with retry on
flakiness).

## What to verify when contributing

- New logic has unit or property coverage.
- Behavior changes are exercised at the right tier (integration / cluster /
  fault).
- Replay determinism holds: a recorded session in produces a byte-identical
  session out.

## Related

- Contribution and CI gate: [../CONTRIBUTING.md](../CONTRIBUTING.md#tests-and-benchmarks).
- Determinism: [../docs/concepts/determinism-and-consensus.md](../docs/concepts/determinism-and-consensus.md).
