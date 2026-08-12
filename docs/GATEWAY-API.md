# exc-gateway-rest API Reference

excoredum is a deterministic, replicated spot exchange matching engine built on Aeron Cluster
(Raft). `exc-gateway-rest` is the HTTP/JSON + WebSocket edge in front of the cluster: **writes**
(users, balances, symbols, orders) go through the `exc-client` SDK with idempotent retry and
leader-change resend; **reads** (order books, user state, balances) come from an embedded read
replica and are eventually consistent. This document covers the complete HTTP and WebSocket
integration surface for front-end and client developers.

The gateway exposes two REST namespaces plus one WebSocket channel, all on the same port:

- `/syncTradeApi/v1` - trading: ping, time, info, order book, place / move / cancel orders, user state, user history
- `/syncAdminApi/v1` - administration: create users, adjust balances, register assets and symbols
- `ws://host:port/ticks-websocket` - real-time market data: trade ticks, order updates, L2 snapshots

---

## Table of Contents

1. [Quick Start](#1-quick-start)
2. [Running the Gateway](#2-running-the-gateway)
3. [GatewayConfig Reference](#3-gatewayconfig-reference)
4. [Response Envelope and HTTP Status](#4-response-envelope-and-http-status)
5. [Error Codes](#5-error-codes)
6. [Symbol and Order Concepts](#6-symbol-and-order-concepts)
7. [Use Cases](#7-use-cases)
8. [Trade API Reference](#8-trade-api-reference)
9. [Admin API Reference](#9-admin-api-reference)
10. [Order Object and States](#10-order-object-and-states)
11. [WebSocket Real-Time Channel](#11-websocket-real-time-channel)
12. [Numeric Conventions](#12-numeric-conventions)
13. [Consistency and Guarantees](#13-consistency-and-guarantees)

---

## 1. Quick Start

The fastest path: start a single-node cluster and the gateway, then run one request from the
command line and one from the browser.

```bash
# Terminal 1: single-node localhost cluster (ingress 20100, member archive 20104)
./gradlew exc-launcher:run

# Terminal 2: REST gateway bound to port 8080
./gradlew :exc-gateway-rest:run
```

```bash
curl http://localhost:8080/syncTradeApi/v1/ping
# {"ticket":0,"gatewayResultCode":0,"coreResultCode":0,"description":"OK","data":null}
```

From a browser (or any JavaScript client), every endpoint is a plain `fetch` against JSON:

```javascript
const base = "http://localhost:8080";

async function api(path, method = "GET", body = undefined) {
  const res = await fetch(base + path, {
    method,
    headers: body === undefined ? {} : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: res.status, envelope: await res.json() };
}

// Health probe
const { status, envelope } = await api("/syncTradeApi/v1/ping");
// status=200, envelope.gatewayResultCode=0
```

> The gateway needs a running cluster and the cluster needs assets/symbols/users registered through
> the admin API before trading. See [Use Cases](#7-use-cases) for the full onboarding flow.

---

## 2. Running the Gateway

```bash
# Defaults describe a single-node localhost cluster (see exc-launcher)
./gradlew :exc-gateway-rest:run

# Full control: HTTP port, cluster ingress endpoints, member archive to follow
./gradlew :exc-gateway-rest:run \
    --args="--port=8080 --ingress=0=localhost:20100 --archive=aeron:udp?endpoint=localhost:20104 \
            --clientId=1 --gatewayId=1"
```

### Launcher arguments

| Argument      | Default                              | Notes                                                                                  |
|---------------|--------------------------------------|----------------------------------------------------------------------------------------|
| `--port`      | `8080`                               | HTTP + WebSocket port.                                                                 |
| `--ingress`   | `0=localhost:20100`                  | Cluster ingress endpoints, Aeron form: `0=host:port,1=host:port,...`.                  |
| `--archive`   | `aeron:udp?endpoint=localhost:20104` | Member Archive control channel the embedded read replica follows.                      |
| `--clientId`  | per-process unique                   | Cluster client identity (dedup scope). A pinned id replays the dedup table on restart. |
| `--gatewayId` | `1`                                  | Instance id (15-bit) minted into the order ids this gateway generates.                 |
| `--aeronDir`  | embedded driver                      | Attach the cluster client to an existing media driver directory instead.               |

> **`--clientId` default**: the launcher mints a per-process unique id so gateway restarts start a
> fresh dedup identity. Pinning a previously used id would replay the cluster's dedup table (cached
> results, commands silently not applied), so only pin `--clientId` when that replay is intended.

`RestGateway` can also be embedded programmatically:

```java
GatewayConfig config = GatewayConfig.builder(
                /*clientId*/ 1L,
                /*ingress*/ "0=localhost:20100",
                /*archive*/ "aeron:udp?endpoint=localhost:20104",
                /*replicaAeronDir*/ Files.createTempDirectory("gw-").toString())
        .port(8080)
        .gatewayId(1)
        .build();

try (RestGateway gateway = RestGateway.launch(config)) {
    int bound = gateway.boundPort(); // actual port when port 0 was requested
}
```

---

## 3. GatewayConfig Reference

All options are set on `GatewayConfig.builder(clientId, ingressEndpoints, archiveControlChannel,
replicaAeronDirectoryName)`. Values below are the builder defaults; the launcher overrides a
subset via command-line arguments.

| Option                        | Type     | Default                        | Notes                                                                                          |
|-------------------------------|----------|--------------------------------|------------------------------------------------------------------------------------------------|
| `clientId`                    | `long`   | required                       | Cluster client identity; scopes the dedup window.                                             |
| `ingressEndpoints`            | `String` | required                       | Aeron cluster client form: `0=host:port,1=host:port,...`                                      |
| `archiveControlChannel`       | `String` | required                       | Member Archive the embedded read replica follows.                                              |
| `replicaAeronDirectoryName`   | `String` | required                       | Media driver directory for the embedded read replica (its own driver).                         |
| `port`                        | `int`    | `8080`                         | HTTP + WebSocket port; `0` picks a free port.                                                  |
| `gatewayId`                   | `int`    | `1`                            | 15-bit instance id mixed into minted order ids.                                                |
| `clientAeronDirectoryName`    | `String` | `null`                         | `null` launches an embedded media driver for the client.                                       |
| `egressChannel`               | `String` | `aeron:udp?endpoint=localhost:0` | Result channel the client binds. Override when client is off-host.                           |
| `requestTimeoutNs`            | `long`   | `10_000_000_000` (10 s)        | Gateway deadline per request; a miss answers 504, the command keeps retrying.                  |
| `maxInFlight`                 | `int`    | `1024`                         | In-flight command window; excess load answers 503.                                             |
| `maxContentLength`            | `int`    | `64 * 1024`                    | Maximum HTTP request body size.                                                                |
| `requestSlots`                | `int`    | `1024`                         | Pooled request slots; must be a power of two.                                                  |
| `websocketPath`               | `String` | `/ticks-websocket`             | WebSocket upgrade path for the real-time channel.                                              |
| `maxWebSocketConnections`     | `int`    | `256`                          | Maximum concurrent WebSocket connections.                                                      |
| `maxSubscriptionsPerConnection` | `int`  | `32`                           | Maximum subscriptions per connection.                                                          |
| `maxWebSocketFrameLength`     | `int`    | `64 * 1024`                    | Maximum WebSocket frame payload size.                                                          |
| `wsInboundSlots`              | `int`    | `1024`                         | Pooled WebSocket event slots; must be a power of two.                                          |

---

## 4. Response Envelope and HTTP Status

Every HTTP response body is a fixed envelope (mirroring the reference `RestGenericResponse` shape):

| Field               | Type   | Meaning                                                                                   |
|---------------------|--------|-------------------------------------------------------------------------------------------|
| `ticket`            | `long` | Reserved correlation slot; always `0` in v1.                                              |
| `gatewayResultCode` | `long` | Gateway-level error code; `0` = OK, otherwise one of [Error Codes](#5-error-codes).        |
| `coreResultCode`    | `long` | Cluster result code on write commands; `0` = `SUCCESS`. Always `0` on read endpoints.      |
| `description`       | `string` | Human-readable result or error description.                                              |
| `data`              | any    | Payload: an object, a number, or `null` on errors.                                         |

```json
{"ticket":0,"gatewayResultCode":0,"coreResultCode":0,"description":"OK","data":null}
```

### HTTP status mapping

| HTTP status | When to expect                                                                          |
|-------------|-----------------------------------------------------------------------------------------|
| `200 OK`    | Successful reads (`ping`, `time`, `info`, order book, user state, user history) and `PUT` / `DELETE` order operations. |
| `201 Created` | Successful writes: create user, adjust balance, create asset, create symbol, place order. |
| `400 Bad Request` | Validation failed (body fields, precision, price, size, action) or the cluster rejected the command (`coreResultCode != 0`, e.g. insufficient balance). The `gatewayResultCode` names the reason. |
| `404 Not Found` | Unknown route, unknown symbol (`1007`), or unknown user (`1010`).                        |
| `503 Service Unavailable` | Gateway capacity exhausted: request slots full, inbound queue full, or the in-flight window is full. Retry later. |
| `504 Gateway Timeout` | No cluster result within `requestTimeoutNs`. **The command is not lost**: the client keeps retrying it idempotently, so a timeout means the response was lost, not the command. |

> **On any non-`200`/`201` response, `data` is `null`.** Always check `gatewayResultCode` (and for
> writes `coreResultCode`) rather than matching on the HTTP status alone.

---

## 5. Error Codes

`gatewayResultCode` values carried in the envelope. Codes mirror the reference
`exchange-gateway-rest` numbering; WebSocket codes start at `2000`.

| Code | Name                    | Meaning                                                    | HTTP status |
|-----:|-------------------------|------------------------------------------------------------|-------------|
| 0    | OK                      | Success                                                    | 200 / 201   |
| 1000 | `SYMBOL_ALREADY_EXISTS` | Symbol id or code already registered.                      | 400         |
| 1001 | `UNKNOWN_BASE_ASSET`    | `baseAsset` is not a registered asset.                     | 400         |
| 1002 | `UNKNOWN_QUOTE_CURRENCY`| `quoteCurrency` is not a registered asset.                 | 400         |
| 1003 | `ASSET_ALREADY_EXISTS`  | Asset code already registered.                             | 400         |
| 1004 | `UNKNOWN_CURRENCY`      | Balance adjustment targets an unregistered currency.       | 400         |
| 1005 | `PRECISION_IS_TOO_HIGH` | Decimal has significant digits beyond the asset scale.     | 400         |
| 1006 | `UNKNOWN_SYMBOL`        | Defined for compatibility; not currently returned.         | -           |
| 1007 | `UNKNOWN_SYMBOL_404`    | Symbol not found (trading or order book).                  | 404         |
| 1008 | `INVALID_CONFIGURATION` | Symbol/trade fields violate constraints (see Admin / Trade API). | 400    |
| 1009 | `INVALID_PRICE`         | `price` / `budget` is missing, malformed, negative, or over-precise. | 400 |
| 1010 | `UNKNOWN_USER_404`      | User does not exist (state/history).                       | 404         |
| 1011 | `INVALID_BODY`          | Malformed JSON or missing required body field.             | 400         |
| 1012 | `INVALID_SIZE`          | `size` missing or not positive.                            | 400         |
| 1013 | `UNKNOWN_ROUTE`         | No route matches the method + path.                        | 404         |
| 2000 | `WS_INVALID_OP`         | WebSocket frame is not valid JSON or has an unknown `op`.  | WS error frame |
| 2001 | `WS_UNKNOWN_CHANNEL`    | Defined for compatibility; not currently returned.         | -           |
| 2002 | `WS_LIMIT_EXCEEDED`     | Subscription limit exceeded on this connection.            | WS error frame |

---

## 6. Symbol and Order Concepts

Every symbol pairs a **base asset** with a **quote currency**. On `BTCUSD`, BTC is the base (the
asset being bought and sold) and USD is the quote (the currency paid with). Order sizes count
**lots**; prices are quoted **per lot** in quote-currency units. Two symbol-level parameters,
registered with the symbol, define the trading grid:

| Concept    | Field on `POST /symbols` | Meaning                                                                                          |
|------------|--------------------------|--------------------------------------------------------------------------------------------------|
| Lot size   | `lotSize`                | How many base units make **one lot** - the smallest tradable quantity. An order `size` of `N` means `N` lots. |
| Price step | `stepSize`               | The smallest price increment, in quote units. Prices are per-lot and should be multiples of the step. |
| Maker fee  | `makerFee`               | Fee charged **per lot** filled when your order rests and is later taken (quote units per lot).   |
| Taker fee  | `takerFee`               | Fee charged **per lot** filled when your order takes liquidity (quote units per lot); must be `>= makerFee`. |

Worked example (the quick-start configuration: BTC scale 2, USD scale 2, `lotSize` `"1"`,
`stepSize` `"0.01"`, fees `"0"`):

```text
1 lot  = 1.00 BTC        (lotSize in base asset units)
1 step = 0.01 USD        (stepSize in quote currency units)
```

- `POST /syncTradeApi/v1/symbols/BTCUSD/trade/1/orders` with `{"price":"100","size":10,"action":"ASK"}`
  rests **10 lots = 10.00 BTC** at **100.00 USD per lot** (10,000 price steps).
- A taker buying 4 lots at 100 pays **4 x 100.00 = 400.00 USD** plus the taker fee; the book's
  `askVolumes` are counted in lots.
- The order book endpoint and the WebSocket `orderBook` snapshot report prices per lot at the
  quote scale and volumes in lots, so they line up with the values you sent.

> The gateway renders money as fixed-scale decimals (see [Numeric
> Conventions](#13-numeric-conventions)); lots and steps are the *meaning* behind those decimals,
> not a unit you must convert by hand. With `lotSize` `"1"` one lot equals one base unit, so sizes
> and prices also read as per-BTC values; with a smaller `lotSize` (e.g. `"0.5"`), `size: 10` means
> 10 lots = 5.00 BTC and `price: "100"` means 100.00 USD per 0.5 BTC lot. Fee operands are per lot
> as well: a `takerFee` of `"0.01"` charges 0.01 USD for each lot filled.

---

## 7. Use Cases

### A. Onboard an exchange (assets, symbol, users, balances)

```
The asset/symbol registry lives in gateway memory: register assets and symbols once
after each gateway start. Users and balances live in the replicated core.
```

```bash
# Register two assets: BTC (id 10, scale 2) and USD (id 20, scale 2)
curl -X POST http://localhost:8080/syncAdminApi/v1/assets \
  -H 'Content-Type: application/json' -d '{"assetCode":"BTC","assetId":10,"scale":2}'
# {"ticket":0,...,"data":{"assetCode":"BTC","assetId":10,"scale":2}}   HTTP 201

curl -X POST http://localhost:8080/syncAdminApi/v1/assets \
  -H 'Content-Type: application/json' -d '{"assetCode":"USD","assetId":20,"scale":2}'
# HTTP 201

# Register the BTCUSD spot pair (lot size in base units, step/fees in quote units)
curl -X POST http://localhost:8080/syncAdminApi/v1/symbols \
  -H 'Content-Type: application/json' \
  -d '{"symbolCode":"BTCUSD","symbolId":1,"baseAsset":"BTC","quoteCurrency":"USD",
       "lotSize":"1","stepSize":"0.01","takerFee":"0","makerFee":"0"}'
# {"ticket":0,...,"data":{"symbolId":1,"symbolCode":"BTCUSD",...,"status":"ACTIVE"}}   HTTP 201

# Create users and fund them
curl -X POST http://localhost:8080/syncAdminApi/v1/users -H 'Content-Type: application/json' -d '{"uid":1}'
# HTTP 201, data=1

curl -X POST http://localhost:8080/syncAdminApi/v1/users/1/accounts \
  -H 'Content-Type: application/json' -d '{"transactionId":1,"currency":"BTC","amount":"1000"}'
# HTTP 201, data=1

curl -X POST http://localhost:8080/syncAdminApi/v1/users/1/accounts \
  -H 'Content-Type: application/json' -d '{"transactionId":2,"currency":"USD","amount":"100000"}'
# HTTP 201, data=1
```

Error cases: `1003` (duplicate asset), `1000` (duplicate symbol), `1001` / `1002` (unknown
base/quote asset), `1004` (unknown currency on balance adjustment), `1005` (amount over-precise),
`1008` (symbol constraints, see [Admin API](#9-admin-api-reference)).

### B. Place an order and read the book

```
Writes are acknowledged only after the cluster commits them. Order books and user
state are served by the read replica, so they catch up a moment after the write.
```

```bash
# Rest a maker ask: sell 10 BTC at 100 USD
curl -X POST http://localhost:8080/syncTradeApi/v1/symbols/BTCUSD/trade/1/orders \
  -H 'Content-Type: application/json' \
  -d '{"price":"100","size":10,"action":"ASK","orderType":"GTC"}'
# HTTP 201
# {"ticket":0,...,"data":{"orderId":281474976710657,"size":10,"filled":0,
#   "state":"ACTIVE","userCookie":0,"action":"ASK","orderType":"GTC",
#   "symbol":"BTCUSD","deals":[]}}

# Read the L2 book (replica catches up; poll until the ask appears)
curl "http://localhost:8080/syncTradeApi/v1/symbols/BTCUSD/orderbook?depth=10"
# {"ticket":0,...,"data":{"symbol":"BTCUSD","askPrices":[100.00],"askVolumes":[10],
#   "bidPrices":[],"bidVolumes":[]}}
```

Error cases: `1007` (unknown symbol, HTTP 404), `1009` (missing/over-precise price, e.g.
`"1.234"` on a scale-2 symbol), `1012` (size missing or `<= 0`), `1008` (`action` not `ASK`/`BID`
or `orderType` not `GTC`/`IOC`/`FOK_BUDGET`). A cluster rejection (e.g. insufficient balance)
returns HTTP 400 with `coreResultCode != 0`.

### C. Cross the book

```
A taker BID at 100 buys 4 of the 10 resting asks. The taker's balances move
immediately; the maker's remainder keeps resting.
```

```bash
curl -X POST http://localhost:8080/syncTradeApi/v1/symbols/BTCUSD/trade/2/orders \
  -H 'Content-Type: application/json' \
  -d '{"price":"100","size":4,"action":"BID","orderType":"GTC"}'
# HTTP 201; state=COMPLETED (instantly filled)

# Taker bought 4 BTC at 100: BTC 1000 -> 1004, USD 100000 -> 99600
curl http://localhost:8080/syncTradeApi/v1/users/2/state
# {"ticket":0,...,"data":{"uid":2,
#   "accounts":[{"currency":"BTC","balance":1004.00},{"currency":"USD","balance":99600.00}],
#   "activeOrders":[...]}}

# Maker remainder still rests: ask volume is now 6
curl "http://localhost:8080/syncTradeApi/v1/symbols/BTCUSD/orderbook?depth=10"
# {"ticket":0,...,"data":{"symbol":"BTCUSD","askPrices":[100.00],"askVolumes":[6],...}}
```

### D. Move and cancel

```bash
# Move the remainder up to 105
curl -X PUT http://localhost:8080/syncTradeApi/v1/symbols/BTCUSD/trade/1/orders/<orderId> \
  -H 'Content-Type: application/json' -d '{"price":"105"}'
# HTTP 200; order book ask becomes [105.00]

# Cancel the remainder
curl -X DELETE http://localhost:8080/syncTradeApi/v1/symbols/BTCUSD/trade/1/orders/<orderId>
# HTTP 200; order book ask becomes []

# The gateway keeps a per-user history of its own orders, with per-fill deals
curl http://localhost:8080/syncTradeApi/v1/users/1/history
# {"ticket":0,...,"data":{"uid":1,"orders":[
#   {"orderId":...,"price":100.00,"size":10,"filled":10,"state":"CANCELLED","userCookie":0,
#    "action":"ASK","orderType":"GTC","symbol":"BTCUSD",
#    "deals":[{"party":"MAKER","price":100.00,"size":4}]}]}}
```

---

## 8. Trade API Reference

Base path: `/syncTradeApi/v1`.

| Method | Path                                       | Description                          | Success body                                                      |
|--------|--------------------------------------------|--------------------------------------|-------------------------------------------------------------------|
| GET    | `/ping`                                    | Health probe                         | `{"ticket":0,...,"data":null}`                                    |
| GET    | `/time`                                    | Server time (wall clock of the gateway) | `{"ticket":0,...,"data":{"isoTime":"2026-08-12T09:30:00Z","epoch":1755000000000}}` |
| GET    | `/info`                                    | Exchange info: assets + symbols      | `{"ticket":0,...,"data":{"serverTime":{...},"assets":[...],"symbols":[...]}}` |
| GET    | `/symbols/{symbolCode}/orderbook`          | L2 order book snapshot               | `{"ticket":0,...,"data":{"symbol":"BTCUSD","askPrices":[...],"askVolumes":[...],"bidPrices":[...],"bidVolumes":[...]}}` |
| GET    | `/users/{uid}/state`                       | One user's balances + active orders  | `{"ticket":0,...,"data":{"uid":1,"accounts":[...],"activeOrders":[...]}}` |
| GET    | `/users/{uid}/history`                     | Orders placed through this gateway   | `{"ticket":0,...,"data":{"uid":1,"orders":[...]}}`                |
| POST   | `/symbols/{symbolCode}/trade/{uid}/orders` | Place an order                       | Order result object (see below)                                   |
| PUT    | `/symbols/{symbolCode}/trade/{uid}/orders/{orderId}` | Move an order (reprice)    | Order result object                                               |
| DELETE | `/symbols/{symbolCode}/trade/{uid}/orders/{orderId}` | Cancel an order             | Order result object                                               |

Query parameters:

- `GET /symbols/{symbolCode}/orderbook` accepts `?depth=<levels>` (default: the engine's L2 max,
  `32`; clamped to that maximum).

### GET /syncTradeApi/v1/ping

Liveness probe. No parameters, no body.

```bash
curl http://localhost:8080/syncTradeApi/v1/ping
# {"ticket":0,"gatewayResultCode":0,"coreResultCode":0,"description":"OK","data":null}
```

### GET /syncTradeApi/v1/time

Current gateway wall-clock time as ISO-8601 plus epoch millis. (The gateway is the only place a
clock may appear; it is never used by the matching engine.)

```bash
curl http://localhost:8080/syncTradeApi/v1/time
# {"ticket":0,...,"data":{"isoTime":"2026-08-12T09:30:00.000Z","epoch":1755000000000}}
```

### GET /syncTradeApi/v1/info

Active assets and symbols registered in the gateway:

```bash
curl http://localhost:8080/syncTradeApi/v1/info
# {"ticket":0,...,"data":{
#   "serverTime":{"isoTime":"...","epoch":...},
#   "assets":[{"code":"BTC","scale":2},{"code":"USD","scale":2}],
#   "symbols":[{"symbolId":1,"symbolCode":"BTCUSD","symbolType":"CURRENCY_EXCHANGE_PAIR",
#     "baseAsset":"BTC","quoteCurrency":"USD","lotSize":1.00,"stepSize":0.01,
#     "takerFee":0.00,"makerFee":0.00,"status":"ACTIVE"}]}}
```

### GET /syncTradeApi/v1/symbols/{symbolCode}/orderbook

L2 snapshot from the embedded read replica, shaped as parallel price/volume arrays. Prices are
rendered per lot at the symbol's quote scale; volumes are in lots.

```bash
curl "http://localhost:8080/syncTradeApi/v1/symbols/BTCUSD/orderbook?depth=10"
# {"ticket":0,...,"data":{"symbol":"BTCUSD",
#   "askPrices":[105.00,110.00],"askVolumes":[6,3],
#   "bidPrices":[99.50],"bidVolumes":[2]}}
```

- Unknown symbol: HTTP 404, `gatewayResultCode=1007`.
- If the replica has not replicated the symbol yet (right after gateway start): HTTP 404,
  `gatewayResultCode=1007`, description mentions the replica.

### GET /syncTradeApi/v1/users/{uid}/state

Balances across all assets plus the user's resting orders, served from the read replica.

```bash
curl http://localhost:8080/syncTradeApi/v1/users/1/state
# {"ticket":0,...,"data":{"uid":1,
#   "accounts":[{"currency":"BTC","balance":1000.00},{"currency":"USD","balance":100000.00}],
#   "activeOrders":[{"orderId":...,"price":100.00,"size":10,"filled":6,"state":"ACTIVE",
#     "userCookie":0,"action":"ASK","orderType":"GTC","symbol":"BTCUSD","deals":[]}]}}
```

- Unknown user: HTTP 404, `gatewayResultCode=1010`.

### GET /syncTradeApi/v1/users/{uid}/history

Orders **placed through this gateway instance** with their fill history (deals). Orders placed by
other gateways or raw clients are not tracked.

```bash
curl http://localhost:8080/syncTradeApi/v1/users/1/history
# {"ticket":0,...,"data":{"uid":1,"orders":[
#   {"orderId":...,"price":100.00,"size":10,"filled":10,"state":"COMPLETED","userCookie":0,
#    "action":"ASK","orderType":"GTC","symbol":"BTCUSD",
#    "deals":[{"party":"MAKER","price":100.00,"size":10}]}]}}
```

- Unknown user (never placed an order through this gateway): HTTP 404, `gatewayResultCode=1010`.

### POST /syncTradeApi/v1/symbols/{symbolCode}/trade/{uid}/orders

Place an order. Body fields:

| Field        | Type     | Required | Notes                                                                                  |
|--------------|----------|----------|----------------------------------------------------------------------------------------|
| `action`     | `string` | yes      | `ASK` (sell) or `BID` (buy).                                                           |
| `size`       | `long`   | yes      | Quantity in **lots** (integer); 1 lot = the symbol's `lotSize` in base units (see [Symbol and Order Concepts](#6-symbol-and-order-concepts)); must be `> 0`. |
| `orderType`  | `string` | no       | `GTC` (default), `IOC`, or `FOK_BUDGET`.                                               |
| `price`      | `string` | for GTC/IOC | Limit price **per lot** as a decimal string, e.g. `"100.00"`; non-negative, should be a multiple of `stepSize`. |
| `budget`     | `string` | for FOK_BUDGET | Total spend budget in quote units as a decimal string, e.g. `"1000.00"`; the FOK sweep fills up to this amount. |
| `userCookie` | `long`   | no       | Client-supplied correlation tag, echoed back in responses and WebSocket updates (default `0`). |

Order-type semantics:

- **GTC** - good till cancelled; the unfilled remainder rests on the book.
- **IOC** - immediate or cancel; the unmatched remainder is rejected and never rests.
- **FOK_BUDGET** - fill-or-kill within a budget: fills as much as the `budget` allows in one
  sweep, rejects the rest.

```bash
curl -X POST http://localhost:8080/syncTradeApi/v1/symbols/BTCUSD/trade/1/orders \
  -H 'Content-Type: application/json' \
  -d '{"price":"100","size":10,"action":"ASK","orderType":"GTC","userCookie":42}'
# HTTP 201
# {"ticket":0,...,"data":{"orderId":281474976710657,"size":10,"filled":0,
#   "state":"ACTIVE","userCookie":42,"action":"ASK","orderType":"GTC",
#   "symbol":"BTCUSD","deals":[]}}
```

- Order ids are minted by the gateway: the `gatewayId` occupies the high bits and a monotonic
  sequence the low bits, so ids are unique per gateway instance.
- Validation stops at the first failure: unknown symbol (`1007`) -> action (`1008`) ->
  orderType (`1008`) -> size (`1012`) -> price/budget (`1009`).
- A cluster-level rejection (e.g. insufficient balance) returns HTTP 400 with
  `coreResultCode != 0` and a `description` naming the core result.
- On success the response `state` is `ACTIVE` (resting) or `COMPLETED` (instantly filled).

### PUT /syncTradeApi/v1/symbols/{symbolCode}/trade/{uid}/orders/{orderId}

Reprice a resting order. Body:

| Field   | Type     | Required | Notes                                              |
|---------|----------|----------|----------------------------------------------------|
| `price` | `string` | yes      | New limit price as a decimal string; non-negative. |

```bash
curl -X PUT http://localhost:8080/syncTradeApi/v1/symbols/BTCUSD/trade/1/orders/281474976710657 \
  -H 'Content-Type: application/json' -d '{"price":"105"}'
# HTTP 200
```

- Errors: `1007` (unknown symbol), `1009` (price missing/invalid). Cluster rejection (e.g. order
  no longer on the book) returns HTTP 400 with `coreResultCode != 0`.

### DELETE /syncTradeApi/v1/symbols/{symbolCode}/trade/{uid}/orders/{orderId}

Cancel a resting order. No body.

```bash
curl -X DELETE http://localhost:8080/syncTradeApi/v1/symbols/BTCUSD/trade/1/orders/281474976710657
# HTTP 200
```

- Errors: `1007` (unknown symbol). Cluster rejection (e.g. order already filled) returns HTTP 400
  with `coreResultCode != 0`.

---

## 9. Admin API Reference

Base path: `/syncAdminApi/v1`. All admin endpoints are `POST`.

| Method | Path                        | Description                    | Success body                                                          |
|--------|-----------------------------|--------------------------------|-----------------------------------------------------------------------|
| POST   | `/users`                    | Create a user                  | `{"ticket":0,...,"data":<uid>}`                                       |
| POST   | `/users/{uid}/accounts`     | Adjust a user's balance        | `{"ticket":0,...,"data":<uid>}`                                       |
| POST   | `/assets`                   | Register an asset (gateway-local) | `{"ticket":0,...,"data":{"assetCode":"BTC","assetId":10,"scale":2}}` |
| POST   | `/symbols`                  | Register a symbol (validated, then committed to the cluster) | Symbol spec with `"status":"ACTIVE"` |

### POST /syncAdminApi/v1/users

Creates a user in the replicated core. A user can also be created implicitly by funding them.

Body:

| Field | Type   | Required | Notes                       |
|-------|--------|----------|-----------------------------|
| `uid` | `long` | yes      | Client-chosen user id.      |

```bash
curl -X POST http://localhost:8080/syncAdminApi/v1/users \
  -H 'Content-Type: application/json' -d '{"uid":1}'
# HTTP 201, data=1
```

- Missing `uid`: HTTP 400, `gatewayResultCode=1011` (`INVALID_BODY`).

### POST /syncAdminApi/v1/users/{uid}/accounts

Credits (`amount > 0`) or debits (`amount < 0`) a user's balance in one asset.

Body:

| Field           | Type     | Required | Notes                                                                          |
|-----------------|----------|----------|--------------------------------------------------------------------------------|
| `currency`      | `string` | yes      | Asset code registered via `POST /assets`.                                      |
| `amount`        | `string` | yes      | Signed decimal string at the asset's scale, e.g. `"1000"`, `"-50.25"`.         |
| `transactionId` | `long`   | no       | Accepted for compatibility with the reference API; not used by the gateway (the client mints its own sequence). |

```bash
curl -X POST http://localhost:8080/syncAdminApi/v1/users/1/accounts \
  -H 'Content-Type: application/json' -d '{"transactionId":1,"currency":"BTC","amount":"1000"}'
# HTTP 201, data=1
```

- Errors: `1011` (missing uid/currency/amount), `1004` (unregistered currency), `1005`
  (over-precise amount), `1011` (malformed amount). A core rejection (e.g. balance would go
  negative) returns HTTP 400 with `coreResultCode != 0`.

### POST /syncAdminApi/v1/assets

Registers an asset in the **gateway-local registry** (in memory; re-register after a gateway
restart). The registry is what `GET /info` and decimal scaling use.

Body:

| Field       | Type   | Required | Notes                                        |
|-------------|--------|----------|----------------------------------------------|
| `assetCode` | `string` | yes    | Unique code, e.g. `"BTC"`.                   |
| `assetId`   | `long` | yes      | Numeric id used by the core.                 |
| `scale`     | `long` | yes      | Decimal places of the unit, e.g. `2` for `"0.01"` steps. |

```bash
curl -X POST http://localhost:8080/syncAdminApi/v1/assets \
  -H 'Content-Type: application/json' -d '{"assetCode":"BTC","assetId":10,"scale":2}'
# HTTP 201, data={"assetCode":"BTC","assetId":10,"scale":2}
```

- Errors: `1011` (missing fields), `1003` (asset code already registered).

### POST /syncAdminApi/v1/symbols

Validates a spot symbol and, on success, registers it locally and commits it to the cluster. The
response `status` flips to `ACTIVE` once the cluster acknowledges.

Body:

| Field          | Type     | Required | Notes                                                                 |
|----------------|----------|----------|-----------------------------------------------------------------------|
| `symbolCode`   | `string` | yes      | Unique code, e.g. `"BTCUSD"`.                                         |
| `symbolId`     | `long`   | yes      | Numeric id used by the core.                                          |
| `baseAsset`    | `string` | yes      | Registered base asset code (e.g. `"BTC"`).                            |
| `quoteCurrency`| `string` | yes      | Registered quote asset code (e.g. `"USD"`).                           |
| `symbolType`   | `string` | no       | Only `"CURRENCY_EXCHANGE_PAIR"` (spot) is supported; anything else is rejected. |
| `lotSize`      | `string` | yes      | How many base units form **1 lot** (see [Symbol and Order Concepts](#6-symbol-and-order-concepts)); positive, at base scale. |
| `stepSize`     | `string` | yes      | Price step: the smallest price increment in quote units; positive, at quote scale.       |
| `takerFee`     | `string` | no       | Taker fee **per lot** in quote units at quote scale (default `"0"`), non-negative.       |
| `makerFee`     | `string` | no       | Maker fee **per lot** in quote units at quote scale (default `"0"`), non-negative, must be `<= takerFee`. |
| `marginBuy` / `marginSell` | `string` | no | Margin operands; must be zero in exchange mode (default `"0"`).       |

```bash
curl -X POST http://localhost:8080/syncAdminApi/v1/symbols \
  -H 'Content-Type: application/json' \
  -d '{"symbolCode":"BTCUSD","symbolId":1,"baseAsset":"BTC","quoteCurrency":"USD",
       "lotSize":"1","stepSize":"0.01","takerFee":"0","makerFee":"0"}'
# HTTP 201
# {"ticket":0,...,"data":{"symbolId":1,"symbolCode":"BTCUSD",
#   "symbolType":"CURRENCY_EXCHANGE_PAIR","baseAsset":"BTC","quoteCurrency":"USD",
#   "lotSize":1.00,"stepSize":0.01,"takerFee":0.00,"makerFee":0.00,"status":"ACTIVE"}}
```

- Validation order (stops at first failure): `1011` (missing fields) -> `1008` (unsupported
  `symbolType`) -> `1001` / `1002` (unknown base/quote asset) -> `1008` (lot/step/fee
  constraints, margin must be zero, `takerFee < makerFee` rejected) -> `1000` (duplicate symbol).
- The registry is in memory: if the gateway restarts, symbols (and assets) must be re-registered
  before trading resumes.

---

## 10. Order Object and States

### Order result object

Place / move / cancel responses carry an order object in `data`:

| Field        | Type     | Notes                                                                  |
|--------------|----------|------------------------------------------------------------------------|
| `orderId`    | `long`   | Gateway-minted order id.                                               |
| `size`       | `long`   | Requested quantity in lots; `-1` on move/cancel responses.             |
| `filled`     | `long`   | Filled quantity at result time; `-1` on move/cancel responses.         |
| `state`      | `string` | One of `NEW`, `ACTIVE`, `CANCELLED`, `COMPLETED`, `REJECTED`.          |
| `userCookie` | `long`   | Echo of the placement `userCookie` (default `0`).                      |
| `action`     | `string` | `ASK` or `BID`.                                                        |
| `orderType`  | `string` | `GTC`, `IOC`, or `FOK_BUDGET`.                                         |
| `symbol`     | `string` | Symbol code.                                                           |
| `deals`      | `array`  | Fills as `{"party":"TAKER"|"MAKER","price":...,"size":...}`; empty on the placement response (fills trail via WebSocket `orderUpdate` frames and history). |

### Order state machine

| State       | Meaning                                                          |
|-------------|------------------------------------------------------------------|
| `NEW`       | Submitted, no cluster result yet.                                |
| `ACTIVE`    | Resting on the book after a successful place.                    |
| `COMPLETED` | Fully filled.                                                    |
| `CANCELLED` | Cancelled (fully reduced).                                       |
| `REJECTED`  | Unmatched remainder rejected (IOC / FOK).                        |

A placement response is `ACTIVE` or `COMPLETED`; it is never `NEW` in the response (the response
is written after the cluster result). Symbols carry their own status: `NEW` between registration
and cluster acknowledgement, `ACTIVE` after.

---

## 11. WebSocket Real-Time Channel

The HTTP port also serves a WebSocket channel at `ws://host:port/ticks-websocket` (configurable
via `websocketPath`). The wire format is plain JSON text frames - no STOMP, no SockJS.

### Client -> server operations

| Operation | Frame                                                          |
|-----------|----------------------------------------------------------------|
| Subscribe to ticks | `{"op":"subscribe","channel":"ticks","symbol":"BTCUSD"}` |
| Unsubscribe from ticks | `{"op":"unsubscribe","channel":"ticks","symbol":"BTCUSD"}` |
| Subscribe to order updates | `{"op":"subscribe","channel":"orders","uid":2}` |
| Unsubscribe from order updates | `{"op":"unsubscribe","channel":"orders","uid":2}` |
| Request an L2 snapshot | `{"op":"orderBook","symbol":"BTCUSD","depth":10}` (`depth` optional, max 32) |

### Server -> client frames

| Type          | Frame                                                                                                                          |
|---------------|--------------------------------------------------------------------------------------------------------------------------------|
| `ack`         | `{"type":"ack","op":"subscribe","channel":"ticks","symbol":"BTCUSD"}` (or `"uid":2` for orders)                              |
| `error`       | `{"type":"error","code":1007,"description":"symbol not found: NOPE"}`                                                          |
| `tick`        | `{"type":"tick","symbol":"BTCUSD","price":100.00,"volume":4,"timestamp":1755000000000}`                                       |
| `orderUpdate` | `{"type":"orderUpdate","uid":2,"orderId":...,"symbol":"BTCUSD","price":100.00,"size":10,"filled":4,"state":"ACTIVE","userCookie":0,"action":"BID","orderType":"GTC"}` |
| `orderBook`   | `{"type":"orderBook","symbol":"BTCUSD","askPrices":[100.00],"askVolumes":[6],"bidPrices":[],"bidVolumes":[]}`                  |

Semantics:

- **Ticks are journal-sourced**: one frame per executed trade, pushed through the embedded read
  replica. They cover the whole market (every trade on the symbol), not just orders placed through
  this gateway. The `timestamp` is the leader-assigned timestamp.
- **Order updates are egress-sourced**: one frame per state change (trade, reduce, reject, move,
  cancel) for orders placed through this gateway. Same scope limitation as user history.
- **Order book snapshots** are answered on demand on the requesting connection and are
  byte-identical in shape to the REST order book endpoint.
- Every `subscribe` / `unsubscribe` is acknowledged with an `ack` frame. Invalid or unknown
  operations answer with an `error` frame (`2000`); unknown symbols with `1007`; subscription
  limits with `2002`.

### JavaScript example

```javascript
const ws = new WebSocket("ws://localhost:8080/ticks-websocket");

ws.onopen = () => {
  // Subscribe to BTCUSD trades and to user 2's order updates
  ws.send(JSON.stringify({ op: "subscribe", channel: "ticks", symbol: "BTCUSD" }));
  ws.send(JSON.stringify({ op: "subscribe", channel: "orders", uid: 2 }));
};

ws.onmessage = (event) => {
  const frame = JSON.parse(event.data);
  switch (frame.type) {
    case "tick":
      console.log(`trade ${frame.symbol} @ ${frame.price} x ${frame.volume}`);
      break;
    case "orderUpdate":
      console.log(`order ${frame.orderId} -> ${frame.state} filled ${frame.filled}/${frame.size}`);
      break;
    case "orderBook":
      console.log(frame.askPrices, frame.bidPrices);
      break;
    case "ack":
      console.log(`subscribed to ${frame.channel}`);
      break;
    case "error":
      console.warn(`error ${frame.code}: ${frame.description}`);
      break;
  }
};

// Request a one-off L2 snapshot
ws.send(JSON.stringify({ op: "orderBook", symbol: "BTCUSD", depth: 10 }));
```

Behavioral notes:

- Subscriptions are per connection and bounded by `maxWebSocketConnections` and
  `maxSubscriptionsPerConnection`; exceeding the per-connection limit answers `2002`.
- Tick frames are dropped for consumers whose channel is not writable (slow consumer); the
  connection is not closed.
- A connection that cannot enqueue inbound frames is closed (backpressure against misbehaving
  clients).
- Disconnecting drops all subscriptions of that connection; no server-side cleanup is needed.

---

## 12. Numeric Conventions

| Convention              | Rule                                                                                             |
|-------------------------|--------------------------------------------------------------------------------------------------|
| Money type              | 64-bit signed integer with a fixed scale defined per asset (`scale` on `POST /assets`).          |
| Request decimals        | Decimal quantities are sent as **strings** in request bodies, e.g. `"price":"100.00"`, `"amount":"-50.25"`. |
| Response decimals       | Decimals are rendered as raw JSON numbers at the fixed scale, preserving scale digits, e.g. scale-2 `10000` renders `100.00`. |
| Order sizes             | `size` counts **lots**; 1 lot = the symbol's `lotSize` in base units. Book volumes are in lots. |
| Order prices            | Prices are **per lot** in quote units and should be multiples of `stepSize`; the decimal you send is converted to price steps internally. |
| Precision               | Significant fraction digits beyond the asset scale are rejected: `1005` on balance adjustments, `1009` on price paths. Trailing zeros beyond the scale are allowed. |
| Integer fields          | `uid`, `orderId`, `size`, `filled`, `userCookie`, `transactionId` are plain JSON integers.       |
| Prohibited types        | Never send `double`/`float` for money; the engine and gateway are integer-only.                   |
| Balance sign            | Positive `amount` credits, negative debits. A debit below zero balance is rejected by the core.   |

---

## 13. Consistency and Guarantees

### Writes (all admin + trade mutations)

- Every write is submitted through the `exc-client` SDK: idempotent retry on timeout, resend on
  leader change. The dedup window is keyed on `(clientId, clientSeq)` inside the cluster.
- **A `504 Gateway Timeout` does not mean the command failed.** The gateway gave up waiting for a
  response, but the client keeps retrying the command until the cluster acknowledges it. Retrying
  the client-side action later may observe the command already applied.
- **A `503` means capacity pressure** (request slots or in-flight window full): retry later; no
  command was submitted.
- Order ids are minted by the gateway (`gatewayId` in the high bits, monotonic sequence) because
  the core requires client-assigned ids.

### Reads (order book, user state, balances)

- Reads are served from an embedded read replica following the member Archive; they are
  **eventually consistent** with bounded staleness (typically milliseconds on a live log) and
  never load the consensus path. After a write, poll the read endpoint until the expected value
  appears - the integration tests do exactly this.
- Right after a gateway restart the replica catches up from the Archive; until then reads may
  report symbols as not replicated (HTTP 404, `1007`).

### Scope limits (v1)

- The asset/symbol registry is **gateway-local and in-memory**: after a gateway restart, assets
  and symbols must be re-registered through the admin API. Users and balances are replicated and
  survive restarts.
- User history and WebSocket order updates only track orders **placed through this gateway
  instance**. Orders placed by other clients are visible via ticks and the order book, but not in
  per-user history/order-update feeds.

> **Authentication and rate limiting are out of scope** for the gateway and must be added at the
> edge before any production exposure.

---

See [README.md](../README.md) for running instructions and [ARCHITECTURE.md](ARCHITECTURE.md) for
the component map, wire formats, and determinism rules.
