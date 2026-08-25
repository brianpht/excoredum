# exc-gateway UI - Features and Test Guide

> The bundled browser UI in front of the deterministic CQRS matching engine. It
> talks plain HTTP/JSON (reads and writes) and subscribes to a WebSocket stream
> that the gateway turns into `ReadClient` queries and `ExcClient` commands.
> See `docs/GATEWAY.md` for the full wire protocol and
> `docs/excoredum-ui-mockup.svg` for a visual reference.

---

## Overview

The UI is a single-page app served by `exc-gateway` from `static/` (no framework,
no build step - plain ES modules). It has five views switched from the top nav.

| View      | Module                 | Purpose                                            |
|-----------|------------------------|----------------------------------------------------|
| Markets   | `modules/views/markets.js` | Symbol list with live last/bid/ask; Trade opens Spot |
| Spot      | `modules/views/trading.js` | L2 order book, price chart, trade tape, place-order ticket, open orders |
| Portfolio | `modules/views/portfolio.js` | Balances, reservations, order history, my trades |
| Admin     | `modules/views/admin.js` | Symbols & fees, add symbol, user actions, conservation |
| Ops       | `modules/views/ops.js` | Health, counters, conservation, live egress events |

Data flow: the SPA loads config-driven symbol/currency registries, holds a
WebSocket `/ws` for realtime `L2`, `MARKET_TAPE`, `TRADE`, `REDUCE`, `REJECT`
events, and uses REST for reads (order book, balances, orders, trades,
conservation, health) and writes (place, cancel, move, reduce, admin).

Default registries (script defaults): `BTC/USDT` (id 1) and `ETH/USDT` (id 2),
currencies `BTC` (10) / `USDT` (20), all with `scaleK = 1`. Admin allow-list:
`1,2,811`.

---

## Features by view

### Markets
- Lists every symbol from `GET /api/v1/symbols` (config-driven registry).
- Last price / best bid / best ask from `GET /api/v1/orderbook?symbolId&maxLevels=1`.
- "Trade" selects the symbol and opens the Spot view.

### Spot (trading terminal)
- **L2 order book** (`GET /api/v1/orderbook?symbolId&maxLevels=32`, plus WS `L2`):
  asks (best at bottom) and bids (best at top), cumulative depth bars, and a
  header showing `BID ... SPREAD ...` (or `NO BOOK`).
- **Price chart** (canvas): candles aggregated from the trade tape + L2.
- **Market trade tape** (`GET /api/v1/markettrades?symbolId&limit=100`, plus WS
  `MARKET_TAPE` / `TRADE`): time, price, size, side.
- **Place order ticket** (`POST /api/v1/orders`): type `GTC` / `IOC` /
  `FOK_BUDGET`, side `BUY` / `SELL`, price / size / reserve-bid inputs, a live
  taker-fee estimate, then PLACE ORDER. On success it refreshes the open-orders
  panel; the result code is shown in a toast.
- **Open orders** (`GET /api/v1/users/{uid}/orders/active`): each row has cancel
  (`DELETE /api/v1/orders/{id}`), move (`PATCH ... {price}`), reduce
  (`PATCH ... {size}`). Move/Reduce prompt for a new value.

### Portfolio
- Equity chips (`GET /api/v1/report/conservation` + `singleUserReport`).
- Per-currency balances available / reserved / total (`GET /api/v1/users/{uid}/balances`).
- Funds-reserved breakdown (bid / ask holds, reserved fee).
- Order history (`GET /api/v1/users/{uid}/orders`) and my trades
  (`GET /api/v1/users/{uid}/trades?limit`).

### Admin (operator console)
- Symbols & fees table (config registry).
- Add symbol form (`POST /api/v1/symbols`, admin) - engine-side registration.
- Users & status: load a uid, add user (`POST /api/v1/users`), adjust balance
  (`POST /api/v1/users/{uid}/balance`), suspend / resume
  (`POST /api/v1/users/{uid}/suspend|resume`) - all admin routes.
- Risk & value conservation (`GET /api/v1/report/conservation`).

All admin routes require `X-User-Id` to be a uid in `gateway.admin.uids`
(`Router.requireAdmin`). The UI only attaches that header when the top-bar
"Admin" field is filled (see "Admin identity" below).

### Ops (system health)
- Replica health (`GET /api/v1/health`): applied position, deterministic state
  hash, conservation currency count.
- Client counters: submitted / completed / expired / backpressure.
- Value conservation (client + fees + reserved).
- Live egress events (WS `TRADE` / `REDUCE` / `REJECT`) in a real-time table.

---

## Test coverage matrix

| UI feature | API / WS involved | Test (tag) |
|------------|-------------------|------------|
| Markets lists symbols + navigates to Spot | `GET /symbols`, `GET /orderbook?symbolId&maxLevels=1`, `setSelectedSymbol` | `marketsViewListsSymbolsAndNavigatesToSpot` |
| Spot renders L2 book + tape + ticket | `GET /orderbook`, `GET /markettrades`, WS `L2` | `spotRendersOrderBookAndTape` |
| Place GTC via ticket | `POST /orders` (GTC), UI ticket | `placeGtcViaTicketRestsAndRefreshesOrders` |
| Place IOC (crossing) via ticket | `POST /orders` (IOC), WS `TRADE`, tape | `placeIocViaTicketCrossesAndStreamsTape` |
| FOK_BUDGET selectable + submits | `POST /orders` (FOK_BUDGET) | `fokBudgetSelectableInTicket` |
| FOK fill vs kill engine semantics | `POST /orders` (FOK_BUDGET), `GET /users/812/orders/active`, `filledSize` | `fokBudgetEngineSemanticsViaRest` |
| Cancel / move / reduce | `DELETE /orders/{id}`, `PATCH /orders/{id}` | `cancelMoveReduceViaOpenOrders` |
| Portfolio balances / reservations / history / trades | `GET /users/{uid}/balances`, `GET /users/{uid}/orders`, `GET /users/{uid}/trades`, `GET /report/conservation` | `portfolioReflectsBalancesReservationsHistoryTrades` |
| Admin add symbol / add user / adjust balance / suspend / resume / guard | `POST /symbols`, `POST /users`, `POST /users/{uid}/balance`, `POST /users/{uid}/suspend|resume`, admin guard | `adminAddsSymbolAndManagesUsers` |
| Ops health / counters / conservation / live events | `GET /health`, WS `TRADE`/`REDUCE`/`REJECT` | `opsShowsHealthCountersConservationAndLiveEvents` |
| Boundary: invalid ticket | UI guard (no request) | `invalidTicketShowsErrorToastWithoutSubmitting` |

There are two suites:

| Suite | Tag | Target | Command |
|-------|-----|--------|---------|
| `GatewayUiSmokeTest` | `ui` | in-process cluster + gateway (self-contained) | `./gradlew :exc-tests:uiTest` |
| `DevStackUiFeatureTest` | `uiStack` | externally started dev stack (`scripts/excoredum-dev.sh`) | see below |

---

## Running the UI feature suite

### Against the dev stack (recommended, end-to-end)
```bash
./scripts/excoredum-ui-test.sh            # start clean stack -> build dists -> test -> stop
KEEP=1 ./scripts/excoredum-ui-test.sh     # leave the stack running afterwards
```
The wrapper: tears down any running stack, rebuilds the application
distributions (so the running gateway serves the current UI), starts a clean
stack via `excoredum-dev.sh start`, installs Playwright Chromium once, runs
`:exc-tests:devStackUiTest`, dumps logs on failure, and stops the stack on exit
(unless `KEEP=1`).

Env overrides: `EXC_NODES`, `EXC_HTTP_PORT`, `EXC_RUN_DIR`, `EXC_CLEAN_START`,
`HEADLESS`, `SKIP_BROWSER_INSTALL`, `KEEP`.

### Against an already-running stack
```bash
./gradlew :exc-tests:devStackUiTest \
  -Pexc.gateway.url=http://localhost:8080 -Pexc.ui.headless=true
```

### In-process smoke (no external stack)
```bash
./gradlew :exc-tests:installPlaywrightBrowsers   # once per machine
./gradlew :exc-tests:uiTest
```

Browsers: `installPlaywrightBrowsers` downloads Chromium. Both suites are
opt-in and NOT wired into `check`.

---

## Test design notes (important when extending)

The suite follows the standard e2e pattern: **seed via API, act via UI, assert
via ground truth**. The UI actions (place, cancel, move, reduce, admin forms and
buttons, view navigation, WS observation) are driven through Playwright on the
real UI; the Java/REST part is used only for a deterministic baseline and for
assertions the UI does not surface. Reasons and caveats:

- **No seed in the UI**: a repeatable starting state (symbols, users, balances, a
  resting ask + a crossing fill) is created through the write API in
  `seedBase`. Registration writes are idempotent (`DUPLICATE` /
  `USER_ALREADY_EXISTS` are accepted), so the suite works whether or not the
  stack was pre-seeded.
- **Lazy order book creation**: the engine registers a symbol on `addSymbol` but
  creates the order book only on the first order (`bookForCreate`). Therefore a
  freshly added symbol reports `found:false` in `GET /orderbook?symbolId=N` until
  it has an order. The admin test proves a new symbol is registered + tradable by
  funding the maker and resting an order on it, then asserting `found:true`.
- **Config-driven symbol registry**: `GET /symbols` is the config registry, not
  the engine. A symbol added via the admin UI does not appear in the UI's symbol
  selector, so it cannot be traded through the ticket - the test trades it via
  REST to verify engine registration.
- **FOK result code**: the engine returns `SUCCESS` for both a filled and a
  killed FOK_BUDGET (they differ in `filledSize` and a `REJECT` event). The UI
  ticket only surfaces `resultCode`, so fill-vs-kill is asserted via the write
  result (`filledSize`), not the UI.
- **Admin identity**: `api.js` attaches `X-User-Id` only when `store.adminUid` is
  set. `store.adminUid` comes from the top-bar "Admin" field on its `change`
  event. If it is empty, admin routes return
  `400 missing X-User-Id header for admin endpoint`. The test fills the field
  (`811`) before using the admin console. The suite also asserts this guard
  (a request without the header is rejected).
- **`window.prompt` dialogs**: Move / Reduce / Adjust-balance use
  `window.prompt`; the suite answers them through a Playwright `onDialog`
  handler with a queued value per prompt.
- **Playwright `.click()` on admin action buttons**: in headless, a synthesized
  `mousedown` lands on the button but `mouseup`/`click` can land on a different
  element, so the delegated `wrap` listener does not fire. The suite uses
  `dispatchEvent("click")` for those buttons, which fires the same real click
  handler (the API layer, the admin header, and the engine write all run). The
  button is present, hit-testable, and `pointer-events: auto`; no CSS causes a
  layout shift, so this is a headless input-synthesis quirk rather than a
  confirmed UI defect.

---

## Environment requirements

- JDK 21 (toolchain) and Linux (Aeron media driver).
- Playwright Chromium binaries (`installPlaywrightBrowsers`).
- When running the dev-stack suite, ensure ports `20100/20104/44000/8080` are
  free and that `/tmp/excoredum` (or `EXC_RUN_DIR`) is writable.

---

## Related

- `docs/GATEWAY.md` - full REST + WebSocket wire contract and gateway internals.
- `docs/ARCHITECTURE.md` - component map, determinism rules, order-book semantics.
- `docs/excoredum-ui-mockup.svg` - visual mockup of the screens.
