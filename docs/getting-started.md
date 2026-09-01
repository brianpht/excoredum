# Getting started

Run justrade locally, from a single in-process example to a full cluster with a
gateway, and place your first order. New to exchange terminology? Read
[concepts/exchange-101.md](concepts/exchange-101.md) first.

## Prerequisites

- JDK 21 LTS (the build enforces the toolchain; you do not need it on your PATH,
  but a matching JDK is required to run).
- Linux. The Aeron media driver is required for cluster, read, bench, and
  end-to-end runs.
- Use the Gradle wrapper (`./gradlew`); no global Gradle install is needed.

Clone and build:

```bash
git clone https://github.com/justrade-io/justrade.git
cd justrade
./gradlew build
```

## 1. Run the in-process example

The fastest way to see the engine work end to end. This starts a single-node
in-process cluster, drives it through the client SDK, and prints every egress
event (trades, reduces, rejects).

```bash
./gradlew :examples:run
```

Source: [examples/.../QuickStartExample.java](../examples/src/main/java/io/justrade/examples/QuickStartExample.java).

## 2. Drive the engine directly (no Aeron)

The engine has no Aeron dependency, so you can call it from a decoded command in
a plain unit-test style. This is the smallest possible surface: a matching engine
and a command.

```java
MatchingEngine engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
CommandOutcome outcome = new CommandOutcome();
boolean duplicate = engine.process(commandDecoder, /* leader timestamp */ 1_000L, outcome);
```

See [concepts/order-book-matching.md](concepts/order-book-matching.md) for what
happens inside `process`.

## 3. Run a single-node cluster

Bring up one localhost cluster node (media driver, archive, consensus,
container):

```bash
./gradlew :launcher:run
```

Attach a CQRS read replica that follows the member's archive and answers queries:

```bash
./gradlew :read:run --args="--archive=aeron:udp?endpoint=localhost:20104"
```

## 4. Run the full dev stack

The dev script starts a 3-node Raft cluster, a read replica, and the
HTTP/WebSocket gateway, and can seed sample data.

```bash
# Start cluster + read replica + gateway
./scripts/justrade-dev.sh start

# Seed a sample symbol and two funded users
./scripts/justrade-dev.sh seed

# Place a resting GTC ask over the REST API (gateway on :8080)
curl -s -X POST http://localhost:8080/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"symbolId":1,"orderId":100,"ask":true,"type":"GTC","price":100,"size":10,"reserveBidPrice":0,"uid":811,"userCookie":1}'

# Stop everything
./scripts/justrade-dev.sh stop
```

The full REST and WebSocket surface is documented in
[API-USAGE.md](API-USAGE.md), with a machine-readable contract in
[openapi.yaml](openapi.yaml) and the gateway design in [GATEWAY.md](GATEWAY.md).

## 5. Containerized end-to-end system test

Bring up a 3-node cluster, a read replica, a write-load generator, and a read
verifier in containers. Exit code 0 means all checks passed.

```bash
docker compose -f docker/docker-compose.yml up --build
```

Workload knobs are set via environment variables, for example
`JUSTRADE_SYMBOLS=4` (see [Configuration](#configuration)).

## Configuration

Engine and ledger capacities are read at launch, not hardcoded, so a deployment
can be sized without rebuilding:

- `CoreConfig` capacities via `justrade.core.*` properties (`--config=<file>` on
  the launcher, `--core-config=<file>` on the read launcher) or
  `-Djustrade.core.*` system properties.
- Aeron ingress term length via `justrade.aeron.termLength` (default `64k`).
- Read-side ledger caps via `--ledger-max-orders-per-user` and
  `--ledger-max-market-trades`.
- Workload shape via `--ops` / `--users` / `--symbols` on the bench runners, or
  `JUSTRADE_OPS` / `JUSTRADE_USERS` / `JUSTRADE_SYMBOLS` in containerized and
  Ansible deployments.

Full capacity table: [ARCHITECTURE.md](ARCHITECTURE.md#configuration). Sizing
guidance: [../deploy/aws/PERFORMANCE.md](../deploy/aws/PERFORMANCE.md).

## Testing

```bash
./gradlew test                   # unit + property tests
./gradlew integrationTest        # in-process single-node cluster
./gradlew clusterTest faultTest  # multi-node and fault-injection suites
```

## Where to go next

- Understand the domain: [concepts/](concepts/README.md).
- Understand the design: [ARCHITECTURE.md](ARCHITECTURE.md).
- Use the API: [API-USAGE.md](API-USAGE.md).
- Deploy it: [Docker Compose](../docker/docker-compose.yml) or
  [AWS](../deploy/aws/README.md).
- Benchmark it: [BENCHMARKING-XCORE.md](BENCHMARKING-XCORE.md).
- Contribute: [../CONTRIBUTING.md](../CONTRIBUTING.md).
