# exc-gateway Audit - 2026-08-27

> Focused audit of the `exc-gateway` module: the Netty HTTP/JSON + WebSocket
> boundary in front of the deterministic CQRS matching engine. This module is
> deliberately **not** the hot path (see `exc-gateway/build.gradle.kts`), so the
> core hot-path / determinism rules from `.github/copilot-instructions.md` do
> not apply; instead this audit checks correctness, thread-safety, resource
> lifecycle, input validation, and the auth boundary. Every finding was located
> by reading all 41 main + 5 test sources and verified against the current
> working tree.

## Method

1. Read every `exc-gateway` main source and unit test, plus the SDK boundaries it
   depends on (`ReadClient`, `ExcClient`, and their listener interfaces).
2. Cross-checked the implemented routes, DTOs, and error codes against
   `docs/GATEWAY.md`, `docs/openapi.yaml`, and `docs/API-USAGE.md`.
3. Traced the threading model (Netty event loops vs the read/write pump threads)
   for cross-thread reads and shutdown races.
4. Verified the fixes with `:exc-gateway:test` and the e2e integration test.

## Executive summary

| Severity | Count | Summary |
|----------|-------|---------|
| P1       | 2     | Admin auth spoofable via a plain `X-User-Id` header (uid is not a secret); default bind `0.0.0.0`; trading endpoints trust caller-supplied `uid` |
| P2       | 3     | Data race reading `ReadClient` diagnostics from the Netty thread; WebSocket fan-out has no backpressure; malformed `X-User-Id` surfaced as `500` |
| P3       | 9     | No HTTP keep-alive; pump `close()` raced `poll()`; DTO primitives conflated absent with `0`; `modifyOrder` accepted both price+size; `Locale`-unsafe `toUpperCase`; dead `SymbolRegistry.byId`; silent `MarketPump` failures; no `GatewayConfig` validation; unnecessary `ConcurrentHashMap` in the bridges |

**Structural quality is strong.** The single-thread ownership of `ReadClient` /
`ExcClient` via two pumps is correct (submit, then register, then poll on one
thread), the egress listener copies the reused `OrderBookSnapshot` holder before
it is overwritten, and the broadcaster tolerates a failing sink. The findings
are concentrated in **auth**, **cross-thread diagnostics**, and **boundary
validation**, not in the concurrency model.

## P1 findings (fixed)

### F-G1 - Admin auth is spoofable; default bind is `0.0.0.0`

`http/Router.java` (old `requireAdmin`), `config/GatewayConfig.java` (default
`httpHost`), `GatewayLauncher.java`.

Admin routes (`/symbols`, `/users`, `/users/{uid}/balance`, suspend/resume)
gated on an `X-User-Id` header matched against `gateway.admin.uids`. A uid is
not a secret - it appears in public trade data and is the same value any caller
supplies on normal trading endpoints - so anyone who can reach the gateway
(default bind `0.0.0.0`) could mint users, adjust balances, add symbols, and
suspend/resume users. The trading endpoints take `uid` from the body/query with
no authentication at all.

Fix: `GatewayConfig` gains an optional `adminApiKey` (`gateway.admin.apiKey`),
the default bind is now `127.0.0.1`, and `Router.checkAdmin` requires a
matching `X-Api-Key` header (constant-time compare via `MessageDigest.isEqual`)
when a key is configured, before the `X-User-Id` allow-list check. The
allow-list uid still identifies the acting admin; the key authenticates it.

### F-G2 - Malformed `X-User-Id` surfaces as `500`, not `400`

`http/Router.java` (old `requireAdmin`).

`Long.parseLong(uid)` was unguarded, so a non-numeric `X-User-Id` threw
`NumberFormatException`, which `HttpHandler` mapped to `500`. Now parsed inside
a try/catch and re-thrown as `ApiException.badRequest` (`400`).

## P2 findings (fixed)

### F-G3 - Cross-thread read of `ReadClient` diagnostics

`read/ReadPump.java`, `http/Router.java` (`health`).

`Router.health()` called `read.lastAppliedPosition()` / `submitted()` /
`completed()` / `expired()` / `backpressure()` on the Netty event loop, while
`ReadClient` documents "not thread-safe" and updates those `long` fields on the
pump thread. A non-volatile cross-thread read.

Fix: `ReadPump` now snapshots those five values into `volatile long` fields once
per pump loop (on the pump thread, after `client.poll()`) and the getters return
the snapshot. The `ReadClient` is never read from another thread.

### F-G4 - WebSocket fan-out has no backpressure

`http/WebSocketHandler.java`.

`StreamSink` called `channel.writeAndFlush(...)` unconditionally; a stalled
subscriber grows the Netty outbound buffer without bound.

Fix: the sink checks `channel.isWritable()` and drops the frame once the channel
crosses its write watermark (Netty default 32 KB / 64 KB), bounding per-connection
memory.

## P3 findings (fixed)

- **F-G5 - no HTTP keep-alive.** `HttpHandler` attached `ChannelFutureListener.CLOSE`
  to every response. Now `HttpUtil.isKeepAlive(request)` decides whether to close,
  so a polling UI terminal reuses the connection.
- **F-G6 - pump `close()` raced an in-flight `poll()`.** `WritePump.close()` /
  `ReadPump.close()` / `MarketPump.close()` interrupted the thread and immediately
  closed the client while the loop could still be inside `poll()`. Now they
  `thread.join(...)` (bounded) before `client.close()`.
- **F-G7 - DTO primitives conflated absent with `0`.** `PlaceOrderRequest` and the
  admin request DTOs used primitive `int`/`long`/`boolean`, so a missing field
  deserialized to `0`/`false` and was submitted to the engine instead of being
  rejected. Required fields are now boxed (`Integer`/`Long`/`Boolean`) and the
  router rejects a null with `400`.
- **F-G8 - `modifyOrder` accepted both `price` and `size`.** Now rejects with `400`
  when both are present (and still rejects when neither is).
- **F-G9 - `toUpperCase()` without `Locale`.** `placeOrder` now uses
  `toUpperCase(Locale.ROOT)`.
- **F-G10 - dead `SymbolRegistry.byId(long)`.** The unused method cast `long` to
  `int`; removed along with its index field (duplicate-id detection stays).
- **F-G11 - `MarketPump` swallowed refresh failures silently.** `thenAccept` was
  replaced with `whenComplete` that increments a `refreshFailures()` counter.
- **F-G12 - `GatewayConfig` had no validation.** `build()` now rejects
  out-of-range stream ids, non-positive `writeClientId`, blank ingress endpoints,
  negative pump interval, and per-symbol invalid scale factors / fees /
  base==quote, and per-currency invalid id/code/scaleK.
- **F-G13 - `ConcurrentHashMap` in the bridges.** `WriteResultBridge` /
  `ReadResultBridge` register and complete on a single pump thread, so the map is
  now a plain `HashMap` (documented as single-threaded).
- **F-G14 - `GatewayLauncher` did not stop the server on early failure.** The
  `finally` block now always calls `server.stop()` so the boss/worker event-loop
  groups are shut down even when `server.start()` or `marketPump.start()` throws.

## Test coverage added

- `GatewayConfigTest`: default bind `127.0.0.1`, `gateway.admin.apiKey` parsing
  (blank disables), and validation rejections (non-positive scale, maker fee above
  taker fee, base==quote, non-positive client id).
- `AdminGuardTest` (new): missing/wrong `X-Api-Key` -> `401`, malformed
  `X-User-Id` -> `400`, non-admin uid -> `403`, and key-check skipped when no key
  is configured.
- `GatewayEndToEndIntegrationTest`: the gateway is now configured with
  `adminApiKey`; asserts missing key -> `401`, wrong key -> `401`, malformed
  `X-User-Id` -> `400`, `PATCH` with both price+size -> `400`, and `POST /orders`
  missing a required field -> `400`.

## Residual risks (documented, not fixed)

- **Trading endpoints still trust the caller-supplied `uid`.** Per-user
  authentication/authorization is explicitly out of scope (see
  `docs/GATEWAY.md`); the gateway forwards the `uid` from the request. The shared
  `X-Api-Key` protects admin only.
- **`X-User-Id` allow-list is still spoofable when `adminApiKey` is not
  configured.** The key is optional for backward compatibility; operators must set
  `gateway.admin.apiKey` and keep the `127.0.0.1` bind for a real gate.
- **No TLS, rate limiting, or connection limits.** The boundary assumes a trusted
  network; a reverse proxy or service mesh is the intended front door.
- **`eventIndex` is `int` in the egress listener.** The write-client listener
  interface exposes `int eventIndex`, so the uint32 extension field (`eventIndexExt`)
  truncates above `2^31`. This is a `exc-write-client` interface choice, not a
  gateway defect; in practice a command does not sweep billions of orders.
- **`MarketPump.refreshFailures()` is not yet wired to the HTTP surface.** It is a
  diagnostic counter only; a follow-up could fold it into `/health`.

## Verified correct (audited, no action)

- Single-thread ownership of `ExcClient` / `ReadClient` through the two pumps;
  submit -> register -> poll ordering means a fast reply is never missed.
- Egress `OrderBookSnapshot` holder is copied into a fresh map before reuse.
- `StreamBroadcaster` fans out to all sinks and skips a failing sink.
- `HttpHandler` writes the response back on the channel event loop, never blocking
  the pump or a Netty thread.
- DTO/route/error mapping matches `openapi.yaml` (after this round's updates).
