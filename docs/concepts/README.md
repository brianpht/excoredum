# Concepts

Plain-language guides to the trading and systems ideas behind justrade. Start
here if you are new to exchanges or want to understand why justrade is built the
way it is. Every term is also defined in the [glossary](../GLOSSARY.md).

## Suggested reading order

1. [Exchange 101](exchange-101.md) - what a spot exchange is: symbols, bids and
   asks, the order book, matching, funding, and fees.
2. [Order book and matching](order-book-matching.md) - price-time priority and
   how justrade stores and matches orders without allocating.
3. [Order types](order-types.md) - GTC, IOC, and FOK-BUDGET, plus PLACE / CANCEL
   / MOVE / REDUCE.
4. [Risk, funding, and fees](risk-and-fees.md) - direct-exchange spot risk,
   reserves, settlement, maker/taker fees, and value conservation.
5. [Determinism and consensus](determinism-and-consensus.md) - why justrade is a
   deterministic state machine replicated by Raft, with exactly-once semantics.
6. [The read path (CQRS)](cqrs-read-path.md) - how queries and streaming are
   served without touching the matching hot path.

## Where to go next

- Run it: [getting-started.md](../getting-started.md).
- The authoritative design: [ARCHITECTURE.md](../ARCHITECTURE.md).
- Design decisions: [decisions/](../decisions/).
