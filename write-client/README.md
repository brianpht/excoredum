# write-client

The write-side SDK: submit commands to the cluster and receive results and egress
events. It depends only on `protocol` (no `core`), so it is a thin, embeddable
client.

## Responsibility

- Submit `CommandEnvelope` messages to the cluster ingress.
- Handle leader changes and reconnects transparently.
- Provide idempotent retry via `(clientId, clientSeq)` correlation, so a retry
  after a timeout or failover cannot double-apply.
- Deliver egress events: command results, trades, reduces, rejects, L2, and
  per-command fills, with backpressure.

## Key classes

- [WriteClient.java](src/main/java/io/justrade/write/client/WriteClient.java) -
  the client: connect, submit, poll.
- `ResultHandler` - callback for command results.
- `TradeEventListener` - callback for trade / reduce / reject events.
- `PendingCommand` - in-flight command tracking for correlation and retry.

## Usage shape

Submit a command and poll for results and events in a closed loop:

```java
try (WriteClient client = WriteClient.connect(config)) {
    client.submitPlaceOrder(...);   // stamped with the next clientSeq
    while (running) {
        client.poll();              // drives result and event callbacks
    }
}
```

See [../examples/](../examples/) for a runnable end-to-end use and
[../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) for the retry and correlation
model.

## Related

- Exactly-once semantics: [../docs/concepts/determinism-and-consensus.md](../docs/concepts/determinism-and-consensus.md).
- Wire format: [../protocol/README.md](../protocol/README.md).
