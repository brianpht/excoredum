# Architecture Decision Records

This directory is the architectural source of truth for excoredum. Per
`.github/copilot-instructions.md`, implementation rules and the performance
budget live here (never in `/docs/sessions`, and nothing else is treated as the
authoritative source).

## Index

- [performance-budget.md](performance-budget.md) - per-service latency and
  allocation budgets, which override the defaults in `.github/copilot-instructions.md`.
- [0001 - Hot agents are not pinned to isolated cores](0001-threading-affinity.md) -
  the affinity library is declared but unused by design; pinning is a deployment concern.
- [0002 - Ask FOK-BUDGET orders pre-validate settlement before matching](0002-fok-budget-ask-settlement.md) -
  walked proceeds are checked against the fee floor and the 64-bit range before
  the book is mutated; settlement failures are never overwritten with SUCCESS.

## Conventions

- Records are ASCII only (no em-dashes, no emojis).
- Diagrams are Mermaid.
- An ADR is immutable once merged; supersede it with a new ADR rather than
  editing its decision.
