# xcore-bench

Comparative benchmarks against upstream exchange-core 0.5.3. This is where
justrade's ported matching semantics are validated for parity and measured for
latency at three layers. It is exempt from the engine's determinism rules because
it deliberately drives a third-party engine.

## Responsibility

- Parity test (`book` mode): replay the same deterministic workload through both
  engines; justrade's book is the reference and exchange-core must produce
  identical trade / reject / reduce counters and a byte-identical L2 book, or the
  run fails.
- Comparative latency at three layers:
  - `book` - matching-only path on the caller thread (apples-to-apples).
  - `engine` - full engine dispatch.
  - `e2e` - cluster end-to-end (pays for Raft commit and archive recording).

## Run

```bash
./gradlew :xcore-bench:run --args="--mode=book --commands=3000000 --target-orders=1000 --iterations=3"
./gradlew :xcore-bench:run --args="--mode=engine --warmup=200000 --ops=1000000"
./gradlew :xcore-bench:run --args="--mode=e2e --warmup=20000 --ops=200000"
```

## Fairness

Only `book` is strictly apples-to-apples. The `engine` and `e2e` layers compare
different system shapes; the `e2e` number includes consensus and archive costs
that exchange-core does not have. Read the methodology and caveats before quoting
figures.

## Related

- Methodology and fairness notes: [../docs/BENCHMARKING-XCORE.md](../docs/BENCHMARKING-XCORE.md).
- Performance budget: [../docs/decisions/performance-budget.md](../docs/decisions/performance-budget.md).
- Upstream: [exchange-core](https://github.com/exchange-core/exchange-core).
