# read

The CQRS read replica. It follows a cluster member's Aeron Archive (committed log
and event journal), rebuilds queryable projections, and answers queries over a
plain Aeron request/response protocol, all without touching consensus.

## Responsibility

- Follow the live committed log and the highly-available event journal.
- Dedup on `(logPosition, eventIndex)` for exactly-once event application, even
  across a leader change.
- Rebuild projections: per-user balances, per-user order history, the L2 book,
  the market trade tape, and value-conservation totals.
- Answer `QueryRequest` messages with `QueryResponse`.

## Key classes

- [ReadReplica.java](src/main/java/io/justrade/read/ReadReplica.java) - entry
  point; runs the follower and query responder.
- `LiveLogSubscriber` - consumes the committed log / journal.
- `QueryResponder` - serves queries over Aeron.
- `order/OrderLedger` - per-user order history projection.
- `report/ReportGenerator` - single-user and aggregate reports.

## Run

```bash
# Follow a member's archive and answer queries
./gradlew :read:run --args="--archive=aeron:udp?endpoint=localhost:20104"
```

## Configuration

- Ledger caps via `--ledger-max-orders-per-user` and
  `--ledger-max-market-trades`.
- `CoreConfig` via `--core-config=<file>` or `-Djustrade.core.*`.

## Related

- The read model: [../docs/concepts/cqrs-read-path.md](../docs/concepts/cqrs-read-path.md).
- Read SDK: [../read-client/README.md](../read-client/README.md).
- Design: [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md).
