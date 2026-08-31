# 0001 - Hot agents are not pinned to isolated cores

Status: accepted
Date: 2026-08-22

## Context

`libs.affinity` is declared in `launcher` but unused. The copilot
instructions require hot agents to be pinned via thread affinity to isolated
cores, and list `libs.affinity` in the version catalog for that purpose.

## Decision

The in-repo bootstrap does not pin threads. The embedded media driver and the
single clustered-service agent run with `ThreadingMode.SHARED`, and the matching
service is a single-writer state machine: one agent thread owns all state, so
there is no hot-core contention to resolve inside the process. Thread affinity
is a deployment-level concern (container CPU pinning, cgroup `cpuset`,
`taskset`) and is left to the operator, who knows the host topology.

`libs.affinity` remains declared as a supported option for deployments that want
in-process pinning, but no code path enables it by default.

## Consequences

- No in-process pinning means the default runs anywhere, including containers
  and CI, without requiring root or isolated cores.
- A deployment that runs the agent on a shared, contended core will see higher
  tail latency than the budget allows; that is the operator's tuning knob, not a
  code change.
- The `libs.affinity` dependency is retained (not removed) so a future ADR can
  enable pinning without a version-catalog change.
