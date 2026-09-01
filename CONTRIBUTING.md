# Contributing to justrade

Thanks for your interest in justrade. It is a deterministic, allocation-conscious
spot exchange matching engine, so contributions are held to a higher bar than a
typical application: correctness, determinism, and tail latency are the product.
This guide explains how to propose changes and get them merged.

## Table of contents

- [Ground rules](#ground-rules)
- [Getting set up](#getting-set-up)
- [Development workflow](#development-workflow)
- [The CI gate (run before every commit)](#the-ci-gate-run-before-every-commit)
- [Hot path rules](#hot-path-rules)
- [Commit and pull request conventions](#commit-and-pull-request-conventions)
- [Tests and benchmarks](#tests-and-benchmarks)
- [Documentation and decisions](#documentation-and-decisions)
- [Reporting bugs and requesting features](#reporting-bugs-and-requesting-features)

## Ground rules

- Be respectful. See the [Code of Conduct](CODE_OF_CONDUCT.md).
- Correctness and determinism come first. If a change increases latency
  variance, allocation count in the hot path, GC pressure, or nondeterminism, it
  will be rejected regardless of how small it looks.
- Priority order for any tradeoff: Correctness > Determinism > Tail Latency >
  Mean Latency > Throughput.
- ASCII only in code comments, Javadoc, and Markdown. No em-dashes, no emojis.
  Use ` - ` and plain ASCII. Diagrams are Mermaid, never ASCII art.
- The architectural source of truth is `docs/decisions/`. Do not treat anything
  under `docs/sessions/` (if present) as an implementation rule.

## Getting set up

Requirements:

- JDK 21 LTS (the build enforces this via the Gradle toolchain; do not use a
  different JDK).
- Linux (the Aeron media driver is required for cluster, integration, and
  end-to-end runs).
- No global Gradle install needed; use the wrapper (`./gradlew`).

Clone and build:

```bash
git clone https://github.com/justrade-io/justrade.git
cd justrade
./gradlew build
```

Run the in-process example to confirm your toolchain works:

```bash
./gradlew :examples:run
```

## Development workflow

1. Open an issue first for anything beyond a trivial fix, so design can be
   discussed before code is written.
2. Fork the repository and create a topic branch off the default branch.
3. Make focused changes. Avoid unrelated refactors in the same pull request.
4. Run the full CI gate locally (below). All steps must pass with zero errors
   and zero warnings.
5. Open a pull request using the template and fill in the checklist.

## The CI gate (run before every commit)

Run these in order. All must pass with zero errors and zero warnings. Commits
with failing checks are not accepted. If a step fails, fix it and re-run from
step 1.

```bash
./gradlew spotlessApply                    # 1. auto-fix formatting (run first, not spotlessCheck)
./gradlew checkstyleMain checkstyleTest     # 2. zero lint violations
./gradlew compileJava                       # 3. zero compiler warnings (-Werror is hardcoded)
./gradlew test integrationTest              # 4. unit, property, and integration tests
./gradlew :core:jmh -PquickBench            # 5. benchmark smoke run, no regression > 10% vs baseline
```

CI verifies formatting with `spotlessCheck`; locally you apply with
`spotlessApply`. Multi-node and fault suites (`clusterTest`, `faultTest`) run in
the default `check` gate and in CI.

## Hot path rules

The hot path is the steady-state per-command flow: `decode`, `onEvent`,
`dispatch`, `lookup`, `update`, `encode`, `publish`, `consume`. In these paths:

- No `synchronized`, `ReentrantLock`, or any blocking primitive.
- No `HashMap` / `TreeMap` / `ConcurrentHashMap`; use Agrona primitive maps
  (`Long2ObjectHashMap`, `Int2ObjectHashMap`, ...).
- No boxed types, `Optional`, streams, lambdas that capture state, or
  `instanceof` chains on hot interfaces.
- No `String.format`, string concatenation, or per-event allocation.
- No `System.currentTimeMillis()` / `System.nanoTime()` in tight loops; use the
  injected cached clock.
- Ring and sequence indices use `seq & (capacity - 1)`; capacities are
  power-of-two. Never use `%`.
- Messages are SBE flyweights wrapping a `DirectBuffer`, never per-message POJOs.

The full rule set lives in `.github/copilot-instructions.md`. Any change to a
hot-path file must include JMH numbers (before/after) in the pull request and,
where relevant, an allocation check (`-prof gc`).

## Commit and pull request conventions

- Keep commits focused and logically scoped; prefer small, reviewable diffs.
- Write imperative, present-tense subjects (for example, "add FOK-BUDGET ask
  pre-validation"), under ~72 characters, with a body explaining the why.
- Reference the issue the change addresses.
- Do not push commits, tags, or any refs created by tooling on your behalf that
  you did not intend. Open a pull request from your fork.
- One logical change per pull request. Split large work into a reviewable
  sequence.

## Tests and benchmarks

Every change must be covered by the appropriate test tier:

- Unit and property tests (`./gradlew test`) for pure logic. Property tests use
  jqwik for sequence arithmetic, codec round-trips, and settlement conservation.
- Integration tests (`./gradlew integrationTest`) for the in-process single-node
  cluster.
- Cluster and fault tests (`./gradlew clusterTest faultTest`) for election,
  restart, catch-up, and exactly-once across failover.
- JMH benchmarks for any hot-path change; publish before/after numbers.
- Replay determinism: a recorded session in must produce a byte-identical
  session out.

## Documentation and decisions

- Update user-facing docs when behavior or APIs change (see `docs/` and module
  READMEs).
- Significant or hard-to-reverse design choices are recorded as Architecture
  Decision Records under `docs/decisions/`. An ADR is immutable once merged;
  supersede it with a new ADR rather than editing the decision. See
  [docs/decisions/README.md](docs/decisions/README.md).

## Reporting bugs and requesting features

Use the issue templates:

- Bug report: include reproduction steps, expected vs actual behavior, and any
  determinism or latency impact.
- Feature request: describe the use case and how it fits the current scope
  (single-region, non-sharded spot exchange).

For anything security-related, do not open a public issue. Follow
[SECURITY.md](SECURITY.md).
