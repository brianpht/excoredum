# 0002 - Ask FOK-BUDGET orders pre-validate settlement before matching

Status: accepted
Date: 2026-08-31

## Context

An audit of the FOK-BUDGET port against upstream exchange-core 0.5.3
(`OrderBookNaiveImpl.newOrderMatchFokBudget`, `RiskEngine.placeExchangeOrder`,
`handleMatcherEventsExchangeSell`) confirmed that the matching semantics were
ported faithfully: an ask FOK-BUDGET matches against the ENTIRE opposite side
with no price limit, and the budget is a minimum-proceeds bound. Upstream,
however, applies its ask fee floor (`price * quoteScaleK >= takerFee`, a
per-lot check) to the total budget, and performs no overflow or negativity
checks anywhere in the settle path. Both gaps were inherited by this engine
and combine into two defects for ask FOK-BUDGET orders (the only order type
whose settlement amounts are not bounded by its own price):

1. Walked proceeds can exceed the signed 64-bit range once quote-scaled. The
   two-pass `settleAmountsFit` guard then fails AFTER matching has consumed
   the resting makers, and `handlePlace` overwrote the resulting OVERFLOW
   with SUCCESS: makers lost their orders with holds stranded, the taker's
   base hold was stranded, trade events were journaled for unsettled fills,
   and the dedup table cached SUCCESS. Deterministic, so durable on every
   node and in every snapshot.
2. Proceeds below `takerFee * size` settle to a negative taker quote balance
   (money minted), violating value conservation.

## Decision

Ask FOK-BUDGET settlement capacity is validated against the walked depth
BEFORE the book is mutated, and settlement failures are never overwritten:

- `OrderBookNaive.matchFokBudget` takes the symbol spec. For an ask, the
  existing budget walk additionally rejects with `FOK_KILLED_OVERFLOW` when
  the cost accumulation itself, `walkedCost * quoteScaleK`, or
  `takerFee * size` does not fit a signed long, and with
  `FOK_KILLED_FEE_FLOOR` when the walked proceeds cannot cover
  `takerFee * size`. Both kills emit the usual reject event and touch no
  book state; `MatchingEngine` maps them to `OVERFLOW` and
  `RISK_ASK_PRICE_LOWER_THAN_FEE`, releases the hold, and reports filled 0.
  The exact floor replaces the inherited budget-based per-lot comparison,
  which is dimensionally wrong for `size > 1` in both directions.
- `handlePlace` bounds `(takerFee + makerFee) * size` up front for every
  order type, so `collectFee` can never overflow regardless of how fills
  partition the size (`MAX_FEE` caps make this reachable with large sizes).
- `settleFills` returns its result code; `handlePlace` and `handleMove`
  propagate it instead of unconditionally writing SUCCESS. With the bounds
  above in place the pre-pass is defense in depth rather than a live branch.

## Consequences

- Deviates from upstream exchange-core behavior in both directions: orders
  whose walked proceeds cannot settle safely are now killed up front (upstream
  corrupts balances), and ask FOK-BUDGET orders whose budget is below the
  per-lot fee but whose walked proceeds cover the total fee now fill
  (upstream rejects them at placement). Replay parity with upstream
  (`exc-xcore-bench`) is unaffected because its workload runs zero fees and
  unit scales, where both checks are inert.
- New observable result paths: `OVERFLOW` or `RISK_ASK_PRICE_LOWER_THAN_FEE`
  with filled 0 and a reject event for a killed ask FOK-BUDGET, and OVERFLOW
  at placement for an oversized fee aggregate. All are deterministic and
  idempotent under the dedup window like any other result.
- The same settlement hole upstream is left as-is there; this project's
  correctness mandate (integer-only overflow-checked money math, value
  conservation) takes precedence over parity with that weakness.
