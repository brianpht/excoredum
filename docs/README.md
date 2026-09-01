# justrade documentation

Everything you need to understand, run, use, deploy, and extend justrade, a
deterministic spot exchange matching engine. If you are new, start with the
concept guides; if you know exchanges already, jump to architecture or the API.

## By role

### I want to understand what this is (trader / domain reader)

- [Concepts overview](concepts/README.md) - start here.
- [Exchange 101](concepts/exchange-101.md) - symbols, order book, matching,
  funding, fees.
- [Order types](concepts/order-types.md) - GTC, IOC, FOK-BUDGET.
- [Risk, funding, and fees](concepts/risk-and-fees.md) - spot risk and
  settlement.
- [Glossary](GLOSSARY.md) - every term defined.

### I want to run and use it (developer / integrator)

- [Getting started](getting-started.md) - build and run, from in-process to full
  stack.
- [API usage](API-USAGE.md) - REST and WebSocket guide with examples.
- [OpenAPI contract](openapi.yaml) - machine-readable REST spec.
- [Gateway](GATEWAY.md) - HTTP/JSON and WebSocket boundary design.

### I want to understand the design (systems engineer)

- [Architecture](ARCHITECTURE.md) - the authoritative design: modules, wire and
  snapshot formats, data flows, determinism rules.
- [Determinism and consensus](concepts/determinism-and-consensus.md) - the
  replicated state machine and exactly-once semantics.
- [The read path (CQRS)](concepts/cqrs-read-path.md) - reads and streaming
  without touching consensus.
- [Order book and matching](concepts/order-book-matching.md) - the matching loop
  and data structures.
- [Architecture Decision Records](decisions/README.md) - why key choices were
  made.

### I want to deploy it (operator)

- [Docker Compose](../docker/docker-compose.yml) and
  [AWS (Terraform + Ansible)](../deploy/aws/README.md) - deployment targets.
- [Getting started](getting-started.md#configuration) - launch options and
  capacities.
- [Performance budget (ADR)](decisions/performance-budget.md) - authoritative
  latency and allocation targets.
- [Deployed AWS numbers](../deploy/aws/PERFORMANCE.md) - recorded metrics and
  sizing guidance.

### I want to contribute (contributor)

- [Contributing](../CONTRIBUTING.md) - workflow, the CI gate, and hot-path rules.
- [Code of Conduct](../CODE_OF_CONDUCT.md).
- [Security policy](../SECURITY.md).
- [Benchmarking vs exchange-core](BENCHMARKING-XCORE.md) - methodology and
  fairness notes.
- [Changelog](../CHANGELOG.md).

## Module documentation

Each Gradle module has its own README with purpose, key classes, and how to run
it:

| Module | Description |
|--------|-------------|
| [protocol](../protocol/README.md) | SBE schema and generated flyweight codecs |
| [core](../core/README.md) | Deterministic engine: order book, risk, dedup, snapshot, journal |
| [launcher](../launcher/README.md) | Aeron bootstrap: media driver, archive, consensus |
| [write-client](../write-client/README.md) | Write-side SDK |
| [read](../read/README.md) | CQRS read replica and journal consumers |
| [read-client](../read-client/README.md) | Read-side SDK |
| [gateway](../gateway/README.md) | HTTP/JSON + WebSocket boundary |
| [bench](../bench/README.md) | End-to-end latency harness |
| [xcore-bench](../xcore-bench/README.md) | Comparative benchmarks vs exchange-core |
| [examples](../examples/README.md) | Runnable examples |
| [tests](../tests/README.md) | Unit, property, integration, cluster, fault suites |

## Conventions

- Documentation is ASCII only (no em-dashes, no emojis); diagrams are Mermaid.
- The architectural source of truth is [decisions/](decisions/). Nothing under
  `docs/sessions/` (if present) is an implementation rule.
