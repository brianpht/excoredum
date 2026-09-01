# gateway

The HTTP/JSON and WebSocket boundary. It sits in front of the write and read
SDKs, turning REST calls into commands and queries and streaming market events
over WebSocket. It is explicitly not part of the deterministic hot path, so it
can use ordinary JSON and Netty conveniences.

## Responsibility

- Expose REST endpoints for trading (place / cancel / move / reduce), reads
  (balances, book, trades, reports, symbols, currencies), and admin (user and
  symbol management, suspend / resume).
- Stream market events (trades, rejects, reduces, L2, market tape) over
  WebSocket.
- Translate between JSON at the edge and SBE via the SDKs internally.

## Key classes

- [GatewayLauncher.java](src/main/java/io/justrade/gateway/GatewayLauncher.java) -
  entry point.
- Subpackages: `http/` (Netty server and routing), `write/` (command endpoints),
  `read/` (query endpoints), `stream/` (WebSocket streaming).

## Run

```bash
./gradlew :gateway:run
```

The gateway listens on port 8080 by default. With the full dev stack it is started
for you:

```bash
./scripts/justrade-dev.sh start
```

## API

- Task-oriented guide with examples: [../docs/API-USAGE.md](../docs/API-USAGE.md).
- Machine-readable contract: [../docs/openapi.yaml](../docs/openapi.yaml).
- Boundary design and concurrency model: [../docs/GATEWAY.md](../docs/GATEWAY.md).

## Related

- The read model behind the queries: [../docs/concepts/cqrs-read-path.md](../docs/concepts/cqrs-read-path.md).
