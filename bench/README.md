# bench

End-to-end latency harness. It runs an in-process cluster driven by the client
SDK and records latency with HdrHistogram, reporting tail percentiles (the metric
justrade actually cares about).

## Responsibility

- Drive the deterministic workload through the write client against a cluster.
- Record end-to-end latency and report p50 / p99 / p99.9 / p99.99 / max.
- Provide a repeatable harness for measuring the full command path, not just the
  engine.

## Run

Bench runners accept workload knobs, for example:

```bash
./gradlew :bench:run --args="--ops=1000000 --users=5000 --symbols=256"
```

Environment equivalents `JUSTRADE_OPS`, `JUSTRADE_USERS`, `JUSTRADE_SYMBOLS` are
honored in containerized and Ansible deployments.

## Related

- Targets and budget: [../docs/decisions/performance-budget.md](../docs/decisions/performance-budget.md).
- Comparative benchmarks: [../xcore-bench/README.md](../xcore-bench/README.md).
- Deployed AWS results: [../deploy/aws/PERFORMANCE.md](../deploy/aws/PERFORMANCE.md).
