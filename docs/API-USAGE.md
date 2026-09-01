# gateway HTTP/JSON API - Usage Guide

> Task-oriented how-to for the `gateway` boundary. Pairs with
> [docs/GATEWAY.md](GATEWAY.md) (full wire reference) and
> [docs/ARCHITECTURE.md](ARCHITECTURE.md) (engine + read-side model). The same
> API is also described as a machine-readable contract in
> [docs/openapi.yaml](openapi.yaml).
>
> All money values are **integer fixed-point**, scaled by each symbol/currency's
> `scaleK`. The sample dev config uses `scaleK=1`, so `price:100` means 100 (not
> 0.0001). Keep that in mind when reading the examples.

## 0. Prerequisites

```bash
./gradlew build                      # build everything
./scripts/justrade-dev.sh start     # 3 cluster nodes + read replica + gateway
./scripts/justrade-dev.sh seed      # symbol 1 BTC/USDT + users 811/812 + a resting ask and a fill
```

Base URL: `http://localhost:8080`. The dev script writes logs and PID files
under `/tmp/justrade`; `stop` tears the stack down.

## Identity and admin

- Read and trading endpoints need **no** auth header.
- Admin endpoints (`/symbols`, `/users`, `/users/{uid}/balance`,
  `/users/{uid}/suspend`, `/users/{uid}/resume`) require the `X-User-Id` header
  to carry a uid listed in `gateway.admin.uids` (empty by default; the dev script
  and AWS deployment set it to `1,2,811`). A missing header is `400`; a uid not
  in the allow-list is `403`.
- When `gateway.admin.apiKey` is configured, admin endpoints also require a
  matching `X-Api-Key` header (missing/invalid is `401`). The examples below
  assume `gateway.admin.apiKey=change-me`.

## 1. Fund a user (admin)

```bash
# Create user 812
curl -s -X POST http://localhost:8080/api/v1/users \
  -H 'X-User-Id: 811' -H 'X-Api-Key: change-me' -H 'Content-Type: application/json' \
  -d '{"uid":812}'
#> {"commandIdHi":0,"commandIdLo":123,"resultCode":"SUCCESS","uid":812,"orderId":null,"filledSize":null}
```

```bash
# Fund 1,000,000 quote (currency 20 = USDT)
curl -s -X POST http://localhost:8080/api/v1/users/812/balance \
  -H 'X-User-Id: 811' -H 'X-Api-Key: change-me' -H 'Content-Type: application/json' \
  -d '{"currency":20,"amount":1000000}'
#> {"commandIdHi":0,"commandIdLo":124,"resultCode":"SUCCESS","uid":812,"orderId":null,"filledSize":null}
```

`currency` and `amount` are raw fixed-point values; the read side reports them
back with the currency's `scaleK` in mind.

## 2. Place / cancel / move / reduce an order

Every trading write returns a `WriteResultDto`:

```json
{"commandIdHi":0,"commandIdLo":125,"resultCode":"SUCCESS","uid":811,"orderId":100,"filledSize":null}
```

`resultCode` is the deterministic engine result (`SUCCESS`, `REJECTED_*`, ...).
`uid` / `orderId` / `filledSize` are `null` when the engine does not emit them
(e.g. `filledSize` is `null` for a resting GTC, and a number for an IOC that
filled).

### Place a resting GTC ask

```bash
curl -s -X POST http://localhost:8080/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"symbolId":1,"orderId":100,"ask":true,"type":"GTC","price":100,"size":10,"reserveBidPrice":0,"uid":811,"userCookie":1}'
#> {"commandIdHi":0,"commandIdLo":130,"resultCode":"SUCCESS","uid":811,"orderId":100,"filledSize":null}
```

`type` is `GTC | IOC | FOK_BUDGET`. For GTC the `reserveBidPrice` reserves quote
funds when the order is a bid; for an ask it is `0`.

### Place a crossing IOC (fills immediately)

```bash
curl -s -X POST http://localhost:8080/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"symbolId":1,"orderId":101,"ask":false,"type":"IOC","price":105,"size":6,"reserveBidPrice":105,"uid":812,"userCookie":2}'
#> {"commandIdHi":0,"commandIdLo":131,"resultCode":"SUCCESS","uid":812,"orderId":101,"filledSize":6}
```

### Cancel

```bash
curl -s -X DELETE "http://localhost:8080/api/v1/orders/100?symbolId=1&uid=811"
#> {"commandIdHi":0,"commandIdLo":132,"resultCode":"SUCCESS","uid":811,"orderId":100,"filledSize":null}
```

### Move (change price) and Reduce (change size)

Both use `PATCH /api/v1/orders/{orderId}` with the same body shape; the presence
of `price` moves the order, the presence of `size` reduces it.

```bash
curl -s -X PATCH http://localhost:8080/api/v1/orders/100 \
  -H 'Content-Type: application/json' \
  -d '{"symbolId":1,"uid":811,"price":102}'

curl -s -X PATCH http://localhost:8080/api/v1/orders/100 \
  -H 'Content-Type: application/json' \
  -d '{"symbolId":1,"uid":811,"size":4}'
#> {"commandIdHi":0,"commandIdLo":133,"resultCode":"SUCCESS","uid":811,"orderId":100,"filledSize":null}
```

### Request an L2 snapshot over the write path

This does not trade; it triggers an `L2` egress event on the WebSocket stream:

```bash
curl -s -X POST http://localhost:8080/api/v1/orderbook/1/request \
  -H 'Content-Type: application/json' \
  -d '{"uid":811}'
```

## 3. Read market state

### Order book (L2)

```bash
curl -s "http://localhost:8080/api/v1/orderbook?symbolId=1&maxLevels=10"
#> {"symbolId":1,"found":true,"appliedPosition":2720,"asks":[{"price":100,"size":4,"orders":1}],"bids":[]}
```

`found:false` means the engine does not know the symbol; levels are then empty.

### Market trades (the replicated tape)

```bash
curl -s "http://localhost:8080/api/v1/markettrades?symbolId=1&limit=10"
#> [{"timestamp":1700000000123,"symbolId":1,"price":105,"size":6,"makerOrderId":100,"makerUid":811,"takerUid":812}]
```

### Symbols and currencies (config-driven registry)

```bash
curl -s http://localhost:8080/api/v1/symbols
#> [{"symbolId":1,"name":"BTC/USDT","baseCurrency":10,"quoteCurrency":20,"baseScaleK":1,"quoteScaleK":1,"makerFee":0,"takerFee":0},{"symbolId":2,"name":"ETH/USDT","baseCurrency":10,"quoteCurrency":20,"baseScaleK":1,"quoteScaleK":1,"makerFee":0,"takerFee":0}]

curl -s http://localhost:8080/api/v1/currencies
#> [{"id":10,"code":"BTC","scaleK":1},{"id":20,"code":"USDT","scaleK":1}]
```

### Single-user report (balances + resting orders)

```bash
curl -s http://localhost:8080/api/v1/users/811/balances
#> {"uid":811,"exists":true,"suspended":false,"appliedPosition":2720,"balances":[{"currency":10,"balance":990},{"currency":20,"balance":1000630}],"orders":[{"symbolId":1,"orderId":100,"side":"ASK","price":100,"size":10,"filled":6,"remaining":4,"reserveBidPrice":0}]}
```

`balance` is the user's **available** (free) funds; the amount reserved by a
resting order is the order's `remaining` (here 4 base). To see the system-wide
reserved/available split, use `/api/v1/report/conservation`.

### Order history / active orders / user trades / single order

```bash
curl -s http://localhost:8080/api/v1/users/811/orders        # full lifecycle history
curl -s http://localhost:8080/api/v1/users/811/orders/active # resting orders only
curl -s "http://localhost:8080/api/v1/users/811/trades?limit=10"
curl -s http://localhost:8080/api/v1/orders/100              # one order incl. fills
```

### Value conservation and health

```bash
curl -s http://localhost:8080/api/v1/report/conservation
#> {"appliedPosition":2720,"totals":[{"currency":10,"accountBalance":996,"reserved":4,"fees":0,"total":1000},{"currency":20,"accountBalance":2000000,"reserved":0,"fees":0,"total":2000000}]}

curl -s http://localhost:8080/api/v1/health
#> {"appliedPosition":2720,"stateHash":6168209356638280452,"submitted":30,"completed":29,"expired":0,"backpressure":0,"totals":[{"currency":10,"accountBalance":996,"reserved":4,"fees":0,"total":1000}]}
```

`reserved + accountBalance + fees = total` always - the conservation invariant is
visible through the boundary.

## 4. Stream over WebSocket (`/ws`)

Connect to `ws://localhost:8080/ws`. The gateway broadcasts to **all**
subscribers (the client filters by symbol). Use any WS client, e.g.:

```bash
websocat ws://localhost:8080/ws
```

Place a crossing order (section 2) and you will observe a `TRADE` frame:

```json
{"type":"TRADE","commandIdLo":131,"eventIndex":0,"symbolId":1,"makerOrderId":100,"makerUid":811,"takerUid":812,"price":105,"size":6,"makerCompleted":true}
```

Event types:

| `type`         | Notes                                                        |
|----------------|--------------------------------------------------------------|
| `TRADE`        | one fill; `makerCompleted` tells whether the maker order ended |
| `REDUCE`       | an order was reduced by `reducedBy`                          |
| `REJECT`       | the order was rejected for `rejectedSize`                    |
| `L2`           | periodic / on-request snapshot (`asks`, `bids`)              |
| `MARKET_TAPE`  | periodic batch of `trades[]`                                 |

`L2` and `MARKET_TAPE` are emitted by the `MarketPump` on the configured
interval (`gateway.marketPump.intervalMs`; `0` disables them).

## 5. Admin operations and the guard

```bash
# Add a symbol (admin)
curl -s -X POST http://localhost:8080/api/v1/symbols \
  -H 'X-User-Id: 811' -H 'X-Api-Key: change-me' -H 'Content-Type: application/json' \
  -d '{"symbolId":5,"baseCurrency":30,"quoteCurrency":40,"baseScaleK":1,"quoteScaleK":1,"takerFee":0,"makerFee":0}'

# Suspend / resume a user
curl -s -X POST http://localhost:8080/api/v1/users/812/suspend -H 'X-User-Id: 811' -H 'X-Api-Key: change-me'
curl -s -X POST http://localhost:8080/api/v1/users/812/resume  -H 'X-User-Id: 811' -H 'X-Api-Key: change-me'
```

```bash
# Admin guard: without the X-Api-Key header the route is rejected
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8080/api/v1/symbols \
  -H 'Content-Type: application/json' -d '{}'
#> 401          # missing/invalid X-Api-Key (when gateway.admin.apiKey is set)
#> 400          # missing X-User-Id header
#> 403          # X-User-Id present but not in gateway.admin.uids
```

## Order-type semantics

- **GTC**: rests in the book until filled, cancelled, or reduced. `reserveBidPrice`
  reserves quote funds for bids.
- **IOC**: matches against the opposite side immediately; any unfilled remainder
  is discarded (not rested).
- **FOK_BUDGET**: fill-or-kill within a budget. Unlike IOC it trades up to a
  budget rather than consuming whatever is available to the full size. See
  `docs/GATEWAY.md` and `docs/ARCHITECTURE.md` for the exact rules.

## Error mapping

| HTTP   | Meaning                                                            |
|--------|--------------------------------------------------------------------|
| 200    | Success                                                            |
| 400    | Bad request / malformed input / missing admin header               |
| 401    | Missing or invalid `X-Api-Key` on an admin route                   |
| 403    | Header present but uid is not an allowed admin                     |
| 404    | No route / unknown order                                           |
| 429    | Read/write in-flight window full                                   |
| 500    | Serialization / submit failure                                     |
| 504    | Query or command timeout / expired                                 |

## See also

- `docs/GATEWAY.md` - full REST + WebSocket wire contract and gateway internals
- `docs/ARCHITECTURE.md` - engine, read-side model, and determinism rules
- `docs/openapi.yaml` - the REST surface as an OpenAPI 3.0 spec
