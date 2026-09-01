# Glossary

Domain and systems terms used across justrade. Trading terms are defined the way
justrade implements them; where an exchange convention has variants, the justrade
behavior is authoritative.

## Trading and market structure

- **Spot exchange** - a venue where an asset is bought and sold for immediate
  settlement at the traded price, with no leverage or borrowing. justrade is a
  spot exchange: a buyer must hold the full quote amount and a seller the full
  base amount before trading. See [concepts/exchange-101.md](concepts/exchange-101.md).
- **Symbol** - a tradable market, identified by a numeric `symbolId`. Each symbol
  pairs a base currency and a quote currency (for example BTC/USDT: base BTC,
  quote USDT).
- **Base currency** - the asset being bought or sold (the "BTC" in BTC/USDT).
  Order size is denominated in base units.
- **Quote currency** - the asset used to price the base (the "USDT" in BTC/USDT).
  Price and notional are denominated in quote units.
- **Order book** - the two-sided list of resting buy and sell orders for a
  symbol, organized by price. See [concepts/order-book-matching.md](concepts/order-book-matching.md).
- **Bid** - a buy order. The bid side of the book is buyers willing to pay.
- **Ask** (offer) - a sell order. The ask side of the book is sellers willing to
  sell.
- **Best bid / best ask** - the highest bid and the lowest ask. The gap between
  them is the **spread**.
- **Maker** - an order that rests in the book and provides liquidity. In justrade
  the maker is the order already resting when a trade occurs.
- **Taker** - an incoming order that matches against resting liquidity and
  removes it from the book.
- **Liquidity** - resting orders available to trade against.
- **Fill** - a (partial or full) execution of an order against another order.
- **Trade** - a match between a taker and a maker at the maker's price; both
  sides settle immediately.
- **Notional** - the value of an order in quote currency: `price * size`.
- **Price-time priority** - the matching rule: better-priced orders match first,
  and among equal prices the earliest-arriving order matches first (FIFO).

## Order lifecycle

- **PLACE** - submit a new order.
- **CANCEL** - remove a resting order from the book.
- **MOVE** - change the price of a resting order (re-prioritized to the back of
  the new price level).
- **REDUCE** - decrease the remaining size of a resting order.
- **GTC** (Good-Till-Canceled) - rests in the book until it fills or is canceled.
- **IOC** (Immediate-Or-Cancel) - matches whatever liquidity is available now;
  any unfilled remainder is canceled rather than rested.
- **FOK-BUDGET** (Fill-Or-Kill with budget reserve) - must fill entirely or be
  rejected; for bids, quote funds are budgeted before the order is placed. See
  [concepts/order-types.md](concepts/order-types.md).
- **Reduce event** - an egress event emitted when size leaves the book without a
  trade (a cancel, an IOC remainder, or an explicit reduce).
- **Reject** - a command that is not applied because it fails validation or risk
  (unknown user, suspended user, insufficient funds, invalid price, and so on).

## Risk and settlement

- **Direct-exchange risk** - the spot risk model: users must fully fund orders;
  no margin, no short selling, no leverage. See [concepts/risk-and-fees.md](concepts/risk-and-fees.md).
- **Reserve** - funds locked when an order is placed so the fill can always
  settle; released on cancel or reduce.
- **Settlement** - moving currency between the two parties (and the fee account)
  when a trade occurs.
- **Value conservation** - the invariant that total value in the system is
  unchanged by a trade: what one party loses, the counterparty and the fee
  account gain. A property test asserts taker + maker + fee is constant.
- **Maker / taker fees** - fees charged on the quote side of a fill. The taker
  typically pays a fee; the maker may receive a rebate. Fees accrue to a fee
  account.
- **Fixed-scale integer money** - all amounts are 64-bit integers at a fixed
  scale (never floating point), with overflow checks, so arithmetic is exact and
  reproducible.

## Systems and infrastructure

- **Matching engine** - the deterministic state machine that applies commands to
  the order book and produces trades, reduces, and rejects (`core`).
- **Hot path** - the steady-state per-command flow (decode, match, settle,
  acknowledge) that must be allocation-free, lock-free, and single-writer.
- **Determinism** - identical input logs produce byte-identical state and
  snapshots on every node and every rerun. See
  [concepts/determinism-and-consensus.md](concepts/determinism-and-consensus.md).
- **Idempotency / exactly-once** - a command applies at most once even across
  retries and leader failover, enforced by a per-client dedup window on
  `(clientId, clientSeq)`.
- **Single-writer principle** - each mutable resource is owned by exactly one
  thread, removing the need for locks.
- **Consensus / Raft** - the algorithm Aeron Cluster uses to replicate the
  command log across nodes and agree on order and commit.
- **Aeron** - the messaging and clustering library justrade is built on (media
  driver, Archive, Cluster).
- **Aeron Archive** - durable recording of streams; justrade stores the consensus
  log, snapshots, and the event journal here.
- **Aeron Cluster** - the Raft-based replicated state machine framework.
- **Media driver** - the Aeron transport process (UDP/IPC); required on Linux.
- **SBE** (Simple Binary Encoding) - the zero-copy binary wire format used for
  all messages; codecs are generated flyweights with no reflection.
- **Flyweight** - a codec object that wraps a buffer at an offset and reads or
  writes fields in place, without allocating an intermediate object.
- **Agrona** - the low-level library providing primitive collections, off-heap
  buffers, and concurrency utilities.
- **CQRS** (Command Query Responsibility Segregation) - separating writes
  (commands to the cluster) from reads (queries to a replica). See
  [concepts/cqrs-read-path.md](concepts/cqrs-read-path.md).
- **Read replica** - a process that follows a cluster member's log and answers
  queries without touching consensus (`read`).
- **Journal** - the highly-available, ordered record of committed trade / reduce
  / reject events on the Archive, consumed with exactly-once delivery.
- **Snapshot** - a deterministic dump of engine state in sorted key order with an
  integrity checksum, used for warm restart.
- **Egress** - messages the cluster sends back to clients (results and events).
- **Ingress** - messages clients send to the cluster (commands).
- **Gateway** - the HTTP/JSON and WebSocket boundary over the SDKs (`gateway`),
  not part of the deterministic hot path.
- **L2 book** - aggregated order-book depth by price level (as opposed to
  individual orders), returned to queries and streams.
- **HdrHistogram** - the latency recorder used to report tail percentiles (p50,
  p99, p99.99).
- **Idle strategy** - the wait policy an Aeron agent uses when it has no work
  (for example busy-spin for lowest latency, backoff for balance).
