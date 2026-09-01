# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the project is pre-1.0, minor version bumps may include breaking changes.

## [Unreleased]

### Added

- Open-source governance and community documentation: `CONTRIBUTING.md`,
  `CODE_OF_CONDUCT.md`, `SECURITY.md`, this changelog, issue and pull request
  templates, per-module READMEs, and a `docs/` domain concept guide set.

## [0.1.0] - 2026-09-01

Initial public release.

### Added

- Deterministic, allocation-free spot matching engine with a price-time order
  book and GTC, IOC, and FOK-BUDGET order types (`core`).
- Direct-exchange spot risk with maker/taker fees and integer-only settlement.
- Aeron Cluster (Raft) replication with exactly-once command semantics and a
  per-client dedup window (`launcher`).
- Native deterministic snapshots and a highly-available event journal on the
  Aeron Archive.
- CQRS read replica with order ledger, market trade tape, and a query protocol
  (`read`).
- SBE wire format and generated flyweight codecs (`protocol`).
- Write-side and read-side client SDKs (`write-client`, `read-client`).
- HTTP/JSON and WebSocket gateway over the SDKs (`gateway`).
- Benchmark suites: end-to-end latency harness (`bench`) and comparative
  benchmarks against exchange-core 0.5.3 (`xcore-bench`).
- Deployment automation for Docker Compose and AWS (Terraform + Ansible).

[Unreleased]: https://github.com/justrade-io/justrade/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/justrade-io/justrade/releases/tag/v0.1.0
