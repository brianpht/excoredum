# Benchmark scaling analysis

This document analyzes how to scale the deployed AWS benchmark to larger
command counts, user counts, and symbol counts, and how to size the EC2
instances for a bigger run. It is an analysis and proposal only; it does not
change code.

The deployed benchmark is the `ExternalLoadRunner` / `LoadWorkload` write-side
load plus the `ReadVerifyRunner` read-side verification, driven through Ansible
in `deploy/aws`. The in-process `exc-xcore-bench` module is a separate,
single-JVM engine-vs-exchange-core comparison and is covered at the end.

## Where the knobs live

| Dimension | Ansible var (`group_vars/all.yml`) | Runner flag | Terraform var (`variables.tf`) |
|-----------|------------------------------------|-------------|-------------------------------|
| Commands (write load) | `workload_ops` | `--ops` | - |
| Commands (read verify) | `workload_ops` | `--ops` | - |
| Users | `workload_users` | `--users` | - |
| Parallel load generators | `load_client_id` | `--client-id` | - |
| Symbols | `workload_symbols` | `--symbols` | - |
| Node instance type | - | - | `node_instance_type` |
| App instance type | - | - | `app_instance_type` |
| Cluster node count | - | - | `cluster_node_count` (3 or 5) |

`workload_ops`, `workload_users`, and `workload_symbols` feed `EXC_OPS` /
`EXC_USERS` / `EXC_SYMBOLS` in `deploy/aws/ansible/group_vars/load.yml` and
`verify.yml` (plus `workload_trade_limit` to `EXC_TRADE_LIMIT`), which the bin
entrypoints (`deploy/aws/bin/load.sh`, `verify.sh`) pass to the runners.

## Current state and hard limits

The workload and its read-side verification impose three per-run bounds. They
come from the read replica's `OrderLedger`
(`exc-read/.../read/order/OrderLedger.java`) and the runners' query limits, not
from the matching engine.

1. **Per-user order history**: `OrderLedger.DEFAULT_MAX_ORDERS_PER_USER = 4096`
   (configurable via `ReadReplicaConfig.maxOrdersPerUser` /
   `--ledger-max-orders-per-user`).
   Oldest terminal records are evicted past this. The `LoadWorkload` simulation
   asserts `places(uid)` equals the replica's `orderHistory(uid).size()`, so
   `places(uid) < 4096` must hold for every user.

2. **Per-user trade query**: `ReadVerifyRunner` / `GatewayBenchRunner`
   `--trade-limit` (default 4096). The verifier asserts
   `userTrades(uid, tradeLimit).size() == fills(uid)`, so `fills(uid) <
   tradeLimit` must hold for every user.

3. **Global market trade tape**: `OrderLedger.DEFAULT_MAX_MARKET_TRADES = 65536`
   (configurable via `ReadReplicaConfig.maxMarketTrades` /
   `--ledger-max-market-trades`; a
   ring, oldest trades are overwritten). If the workload produces more than
   65536 total trades, early trades are evicted and the read-side `fills`
   count assertions fail for the affected users.

`LoadWorkload` (`exc-bench/.../bench/LoadWorkload.java`) supports one or many
symbols (`--symbols`, default `1`), a single price level per symbol
(`price(s) = 100 + (s - 1)`, so the single-symbol default is `PRICE = 100`),
size-1 orders, and cycles users round-robin (`uid = 1 + (i % users)`), sharding
symbols round-robin (`s = 1 + (i % symbols)`). All symbols share one base /
quote currency pair. Each iteration is at most one
place for the acting user, so `places(uid) <= ops / users`. Each fill involves
two users (taker and maker), so `fills(uid) <= 2 * places(uid)`.

The engine-side capacities come from `CoreConfig` (defaults below)
(`exc-core/.../config/CoreConfig.java`), overridable at launch via `exc.core.*`
properties (`ClusterLauncher --config`, `ReadServiceLauncher --core-config`)
or `-Dexc.core.*` system properties:

| Capacity | Default | Meaning |
|----------|---------|---------|
| `symbolCapacity` | 1024 | Max symbols (order books) |
| `accountCapacity` | 65536 | Max user accounts |
| `orderPoolCapacity` | 65536 | Max resting orders across all symbols |
| `priceBucketCapacity` | 8192 | Max live price levels across all symbols |
| `dedupClientCapacity` | 4096 | Max distinct clients tracked for dedup |
| `dedupWindow` | 1024 | Per-client dedup window |
| `journalSlotCount` | 65536 | Journal ring slot count (128 bytes each) |
| `eventBufferCapacity` | 1024 | Matcher-event buffer per command |

## Scaling commands (ops)

The write-side load (`ExternalLoadRunner`) is closed-loop with a single client:
it submits, then drains every 16 commands, so its measured throughput is
latency-bound, not a throughput ceiling. To raise the command count:

- Raise `workload_ops` (`group_vars/all.yml`).
- Keep `users >= ops / 2000` if the read-side verifier will also run, so both
  `places(uid)` and `fills(uid)` stay under 4096 (the code comment calls the
  current 100000 ops / 100 users = 1000 ops per user "well inside" the limits).
- Keep total trades under 65536 for read-side verification. Total trades are
  roughly a third of `ops` for this workload, so read-side verification starts
  breaking around the low hundreds of thousands of ops. Past that, either raise
  `maxOrdersPerUser` / `tradeLimit` / `maxMarketTrades`, or run the
  write-side load only (skip `run-verify.yml`) to measure raw throughput.
- For a throughput ceiling rather than a latency number, run several load
  generators concurrently, each with a distinct `load_client_id` (already
  supported; see the README "Saturation load" note).

Minimum users for a target op count (read-side verification enabled):

| ops | min users (`ops / 2000`) | notes |
|-----|--------------------------|-------|
| 100000 | 50 | current default is 100 |
| 500000 | 250 | near the global-tape ceiling for read verify |
| 1000000 | 500 | read verify also needs `maxMarketTrades` raised |
| 3000000 | 1500 | exchange-core scale; read verify needs cap changes |

## Scaling users

- `accountCapacity = 65536` is the hard ceiling, so up to about 65535 users
  work without code changes.
- Setup cost is linear: each user costs `addUser` plus two `adjustBalance`
  commands (3 round-trips). 10000 users means 30000 sequential setup commands,
  which takes minutes; the setup path awaits each one, so large user counts
  need batching (not implemented) or a longer setup window.
- Dedup is per-client, not per-user, so more users do not consume dedup
  capacity. Memory grows roughly linearly with users (two balance records plus
  history per user).

## Scaling symbols (tens to hundreds)

Multi-symbol support is implemented: `LoadWorkload`, `ExternalLoadRunner`, and
`ReadVerifyRunner` take `--symbols` (default 1), shard the round-robin across
symbols, and give each symbol its own resting book and price. The command type
is derived from the per-symbol iteration index (`i / symbols`), not the global
index, so every symbol sees the full place/cancel/reduce cycle regardless of the
symbol count; a symbol count that is a multiple of 8 no longer collapses to a
single command type. Remaining work is the single-symbol `GatewayBenchRunner`.

- **Engine ceiling**: `symbolCapacity = 1024`, so tens to hundreds of symbols
  fit without raising it. Passing 1024 symbols (or needing more resting orders)
  requires a `CoreConfig` override (now exposed via `exc.core.*` in
  `ClusterLauncher` / `ReadServiceLauncher`).
- **Workload**: already parameterized (`--symbols`); `ExternalLoadRunner.setup`
  registers every symbol, and `ReadVerifyRunner` verifies each symbol's L2 plus
  the shared-currency conservation totals.
- **Order pool**: resting orders are global across symbols
  (`orderPoolCapacity = 65536`). The single-price-level workload keeps the
  resting book tiny (a handful of orders per symbol once the command cycle is
  independent of the symbol count), so hundreds of symbols stay well inside the
  default pool. Multi-price-level books would consume more pool and
  `priceBucketCapacity = 8192`.
- **Gateway**: the `gateway_symbols` property is already comma-separated
  (`id|name|base|quote|baseScaleK|quoteScaleK[|makerFee|takerFee]`, parsed in
  `exc-gateway/.../config/GatewayConfig.java`), so the gateway can declare many
  symbols today. `GatewayBenchRunner` still exercises a single symbol.

## Server configuration

- **Instance types** (`variables.tf`): nodes default to `c6i.xlarge` (4 vCPU /
  8 GB), apps to `c6i.large` (2 vCPU / 4 GB). For a bigger run, raise nodes to
  `c6i.2xlarge` or `c6i.4xlarge`; the load generator is single-threaded and
  benefits from a higher-clock instance rather than more cores.
- **JVM heap**: the bin entrypoints (`node.sh`, `read.sh`, `gateway.sh`,
  `load.sh`, `verify.sh`) pin `-Xms` / `-Xmx` with ZGC by default and accept an
  `EXC_JAVA_OPTS` override, so larger runs tune the heap via the Ansible
  `*_java_opts` vars instead of editing scripts.
- **Aeron term length**: `ClusterConfig` reads `exc.aeron.termLength` (default
  `64k`). A larger term (1 MB / 8 MB) reduces flow-control stalls at high rate.
- **Journal ring**: `journalSlotCount = 65536` slots of 128 bytes. If the
  domain-event journal backs up at high throughput (the `journalBackpressure`
  metric climbs), raise the slot count or slot size via a `CoreConfig`
  override.
- **Placement group**: cluster nodes already sit in a cluster placement group
  for consistent inter-node latency. Larger instance types still need available
  capacity in the AZ.

## Comparison with exchange-core

`tmp/exchange-core/README.md` reports a single-symbol benchmark: 3 million
messages distributed as 9% GTC, 3% IOC, 6% cancel, 82% move, with 1000 users,
about 1000 resting orders in about 750 price slots, at constant intervals on a
6-core X5690. Its latency numbers cover only risk processing and matching; they
exclude network, IPC, and journaling.

Two important caveats when comparing:

- The deployed AWS benchmark measures the full end-to-end path (network, Raft
  consensus, journal, archive, read replica), so its numbers are not directly
  comparable to exchange-core's matching-only latency.
- The deployed `LoadWorkload` has a much simpler shape (single price level,
  size 1, place/cancel/reduce mix) than exchange-core's benchmark. For an
  apples-to-apples engine comparison, use `exc-xcore-bench`, whose
  `WorkloadGenerator` is a faithful port of exchange-core's
  `TestOrdersGenerator` and already produces the same distribution (target
  orders, users, sliding price, avalanche IOC, seed).

## Recommended target configurations

For a multi-symbol deployed run:

| Scale | symbols | users | ops | node type | notes |
|-------|---------|-------|-----|-----------|-------|
| Small | 16 | 500 | 1000000 | `c6i.xlarge` | read verify needs `maxMarketTrades` raised (trades ~312K) |
| Medium | 64 | 2000 | 2000000 | `c6i.2xlarge` | read verify needs `maxMarketTrades` raised (trades ~625K) |
| Large | 256 | 5000 | 5000000 | `c6i.4xlarge` | read verify needs `maxMarketTrades` raised (trades ~1.56M) plus a larger read replica heap |

The code-side prerequisites are done: (1) multi-symbol `LoadWorkload` and
verifier support (command type decoupled from the symbol index), (2)
`CoreConfig` overrides through `ClusterLauncher` / `ReadServiceLauncher`, and
(3) JVM heap and Aeron term-length tuning. Each step up then only needs (4)
larger instance types plus a read-side `maxMarketTrades` raise (power of two)
noted per scale - no engine symbol/order-pool override is required because the
resting book stays small.
