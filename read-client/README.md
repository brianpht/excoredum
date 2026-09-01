# read-client

The read-side SDK: query a read replica over plain Aeron request/response
streams. Like `write-client`, it depends only on `protocol`.

## Responsibility

- Send `QueryRequest` messages and correlate `QueryResponse` messages by
  request id.
- Provide idempotent retry with a bounded in-flight window.
- Expose typed results for balances, the L2 book, user reports, order history,
  the trade tape, and value-conservation totals.

## Key classes

- [ReadClient.java](src/main/java/io/justrade/read/client/ReadClient.java) - the
  client: connect, query, poll.
- `BalanceResult` - a decoded balance response.
- `L2Snapshot` - a decoded L2 book response.

## Usage shape

```java
try (ReadClient client = ReadClient.connect(config)) {
    long requestId = client.requestBalance(uid, currencyId);
    while (running) {
        client.poll();   // drives response callbacks, correlated by requestId
    }
}
```

## Related

- The read model: [../docs/concepts/cqrs-read-path.md](../docs/concepts/cqrs-read-path.md).
- Read replica: [../read/README.md](../read/README.md).
