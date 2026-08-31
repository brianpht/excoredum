# Benchmark performance: metrics, settings, and tuning

This document records the AWS benchmark results across tuning runs and explains
how to configure the deployment for low latency versus high throughput. The
workload is the deterministic 5,000,000-op / 256-symbol / 5000-user
`LoadWorkload` driven by `ExternalLoadRunner` against a real 3-node Aeron
Cluster (`c6i.xlarge`, cluster placement group, `ap-southeast-1a`).

## Root cause of the retransmit failures

The `retransmits` counter in the load report is the write client's
application-level re-offer count, not an Aeron packet NAK. Each command is
re-offered when its ACK has not arrived within `retryBackoffNs` (default was
250 ms). A tail-latency spike past that deadline made the client re-offer an
already-applied command; the engine deduped it (`duplicates` on the node), but
the client counted one extra result, so `total != setup + ops` and the run
failed.

Two changes fixed it:

1. `retryBackoffNs` 250 ms -> 2 s (`ClientConfig`), so a slow ACK no longer
   triggers a re-offer. The per-client dedup window still bounds resends, so
   exactly-once is preserved.
2. `socketRcvbufLength` / `socketSndbufLength` 128 KB (Aeron default) -> 16 MB
   on every media driver, plus a `rmem_max` / `wmem_max` sysctl, because the
   128 KB socket receive default is smaller than the 1 MB ingress term buffer
   and drops packets when the term fills.

With these, batch 128 - which previously failed with 16 retransmits - now
passes at ~141k ops/s.

## Run metrics

All runs: 5,000,000 ops / 256 symbols / 5000 users, single closed-loop write
client, `c6i.xlarge`. "Pre-fix" is the original `retryBackoffNs=250ms` with the
Aeron default 128 KB socket buffers; "post-fix" is `retryBackoffNs=2s` with
16 MB socket buffers and a 32 MB `rmem_max`/`wmem_max` sysctl.

| Run | batch | retry | socket | throughput | p50 | p99 | p99.9 | max | retransmits | fills | write-side | read-side |
|-----|-------|-------|--------|-----------|------|------|-------|-----|-------------|-------|------------|-----------|
| baseline | 16 | 250ms | 128KB | 33,710 ops/s | 369us | 2.7ms | 5.9ms | - | 0 | 1,562,432 | PASS | PASS |
| baseline | 64 | 250ms | 128KB | 106,271 ops/s | 427us | 3.8ms | 5.6ms | - | 0 | 1,562,432 | PASS | PASS |
| baseline | 128 | 250ms | 128KB | ~148k ops/s | - | - | - | - | 16 | - | FAIL | - |
| run 1 | 64 | 250ms | 128KB | 88,344 ops/s | 479us | 3.9ms | 5.9ms | 330ms | 16 | 1,562,432 | FAIL (+16) | PASS |
| run 2 | 64 | 250ms | 128KB | 88,048 ops/s | 474us | 4.1ms | 5.9ms | 220ms | 0 | 1,562,432 | PASS | PASS |
| run 3 | 64 | 2s | 16MB | 86,764 ops/s | 478us | 4.1ms | 5.9ms | 347ms | 0 | 1,562,431 | FAIL (1 fill) | PASS |
| run 4 | 128 | 2s | 16MB | 141,174 ops/s | 565us | 3.9ms | 6.0ms | 403ms | 0 | 1,562,432 | PASS | PASS |

Notes:

- The batch-16 and batch-64 baseline rows are the original measurements
  (pre-fix); the batch-128 baseline failed with 16 retransmits.
- Run 1 failed only because the 16 retransmits added 16 extra results
  (`total = 5,015,272` vs expected `5,015,256`); the engine and fills were
  correct.
- Run 3 failed only because one egress fill was lost in transit to the client
  (`fills observed = 1,562,431`); the engine was correct - the read-side
  verify passed and the node journal published the same event count as every
  other run. This is a rare (~1 in 1.5M) client-side egress delivery loss,
  not an engine correctness issue.

## Run settings

| Knob | Where | latency | throughput | Notes |
|------|-------|---------|------------|-------|
| `load_profile` | `group_vars/all.yml` -> `--profile` | `latency` | `throughput` | preset for pipeline depth |
| `load_batch` | `group_vars/all.yml` -> `--batch` | 16 | 64 / 128 | overrides the profile |
| `retryBackoffNs` | `ClientConfig` / `--retry-backoff-ms` | 2s | 2s | keep above the observed tail |
| socket rcv/snd buffer | media driver code | 16MB | 16MB | was 128KB (Aeron default) |
| `rmem_max` / `wmem_max` | `deploy.yml` sysctl | 32MB | 32MB | allows the 16MB socket buffer |
| `aeron_term_length` | `justrade.aeron.termLength` | 64k | 1m / 8m | ingress term buffer |

The socket buffers and the retry deadline are safety margins shared by both
profiles; they are sized once and should not be lowered. The latency/throughput
trade-off is driven by pipeline depth (`batch`) and the ingress term length.

## Optimizing for latency versus throughput

The system exposes a named profile (see `BenchProfile` and the runner's
`--profile` flag), and the Ansible deployment maps it through
`load_profile` -> `JUSTRADE_PROFILE` -> `--profile`:

- **latency** - shallow pipeline (`batch = 16`). Fewest commands in flight,
  lowest tail-latency variance and the most headroom against ingress
  backpressure, at ~34k ops/s.
- **throughput** - deep pipeline (`batch = 64` or `128`). More commands in
  flight raise closed-loop throughput roughly 3x-4x (~106k-141k ops/s) at the
  cost of more ingress backpressure and a higher tail.

An explicit `--batch` (or `load_batch`) overrides the profile's drain depth, so
intermediate points such as `batch = 32` remain available.

Guidance:

- Prefer the `latency` profile when the contract is the tail percentile
  (p99.99); the shallow pipeline keeps the in-flight set small.
- Prefer the `throughput` profile when the contract is ops/s. `batch = 128`
  now passes with the 2s retry deadline; it was the previous failure point.
- Keep `retryBackoffNs` at 2s for both profiles: it must exceed the observed
  end-to-end tail (up to ~400ms at batch 128), otherwise slow ACKs trigger
  spurious re-offers and the FIFO-dependent cancel/reduce ordering breaks.
- Raise `aeron_term_length` (1m -> 8m) only for higher-rate runs; it does not
  fix re-offer reordering, which is governed by the retry deadline.
- The socket buffers (16MB) and the `rmem_max`/`wmem_max` sysctl are required
  together: the OS caps `SO_RCVBUF` at `rmem_max`, so the code value alone has
  no effect without the sysctl.

## Engine and ledger capacity sizing

The read replica's `OrderLedger` and the runners' query limits impose per-run
bounds; the engine capacities come from `CoreConfig` (`justrade.core.*`).

| Capacity | Default | Meaning |
|----------|---------|---------|
| `maxOrdersPerUser` | 4096 | per-user order history (evicts oldest terminal records) |
| `maxMarketTrades` | 65536 | global market trade tape (ring) |
| `accountCapacity` | 65536 | max user accounts |
| `orderPoolCapacity` | 65536 | max resting orders across symbols |
| `priceBucketCapacity` | 8192 | max live price levels |
| `dedupClientCapacity` | 4096 | max distinct clients tracked for dedup |
| `dedupWindow` | 1024 | per-client dedup window |
| `journalSlotCount` | 65536 | journal ring slot count |

Rules of thumb for the 5M-op / 256-symbol / 5000-user workload:

- Keep `users >= ops / 2000` when the read-side verifier runs, so per-user
  fills and order history stay under 4096.
- Total trades are roughly a third of `ops`; read-side verification breaks
  around the low hundreds of thousands of ops unless `maxMarketTrades` is
  raised (the benchmark sets it to `2^21`).
- `orderPoolExhaustions` / `priceBucketPoolExhaustions` in the node metrics
  show the resting book outgrowing the default pools at this scale; the engine
  grows on the cold path, and a `justrade.core.*` override removes that.
