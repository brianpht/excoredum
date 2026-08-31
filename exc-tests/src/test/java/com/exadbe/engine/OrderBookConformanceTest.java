package com.exadbe.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.core.CommandOutcome;
import com.exadbe.core.CommandOutcome.EventKind;
import com.exadbe.engine.orderbook.L2View;
import com.exadbe.engine.orderbook.OrderBookNaive;
import com.exadbe.engine.risk.SymbolSpec;
import com.exadbe.protocol.CommandResultCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pure matching conformance for the naive order book, driven directly (no risk),
 * ported from exchange-core semantics.
 */
class OrderBookConformanceTest {

    private static final int SYM = 1;
    private static final long MAKER = 501L;
    private static final long TAKER = 502L;
    private static final long HIGH_RESERVE = 1_000_000L;

    /** Unit scales, zero fees: MOVE validation passes for every book-level test. */
    private static final SymbolSpec SPEC = new SymbolSpec(SYM, 0, 1, 1L, 1L, 0L, 0L);

    private OrderBookNaive book;
    private CommandOutcome out;

    @BeforeEach
    void setUp() {
        book = new OrderBookNaive(SYM);
        out = new CommandOutcome();
    }

    @Test
    void gtcRestsThenMatchesIncomingTaker() {
        final long restFilled = placeGtc(1L, true, 100L, 10L, MAKER);
        assertEquals(0L, restFilled);
        assertEquals(0, out.eventCount());

        final long filled = placeGtc(2L, false, 105L, 6L, TAKER);

        assertEquals(6L, filled);
        assertEquals(1, out.eventCount());
        final CommandOutcome.EventRecord trade = out.event(0);
        assertEquals(EventKind.TRADE, trade.kind());
        assertEquals(1L, trade.makerOrderId());
        assertEquals(100L, trade.price());
        assertEquals(6L, trade.size());
        assertFalse(trade.makerCompleted());
        assertEquals(1, book.orderCount());
        assertEquals(100L, book.bestAsk());
        assertEquals(Long.MIN_VALUE, book.bestBid());
    }

    @Test
    void iocFillsAvailableAndRejectsRemainder() {
        placeGtc(1L, true, 100L, 10L, MAKER);

        out.reset(0L, 0L);
        final long filled = book.matchIoc(2L, false, 100L, 15L, HIGH_RESERVE, TAKER, out);

        assertEquals(10L, filled);
        final CommandOutcome.EventRecord reject = out.event(out.eventCount() - 1);
        assertEquals(EventKind.REJECT, reject.kind());
        assertEquals(5L, reject.size());
        assertEquals(100L, reject.price(), "reject carries the active order's limit price");
        assertEquals(0, book.orderCount());
    }

    @Test
    void fokBudgetRejectsWhenLiquidityInsufficient() {
        placeGtc(1L, true, 100L, 5L, MAKER);

        out.reset(0L, 0L);
        final long filled = book.matchFokBudget(2L, false, 10_000L, 10L, HIGH_RESERVE, TAKER, SPEC, out);

        assertEquals(0L, filled);
        assertEquals(1, out.eventCount());
        assertEquals(EventKind.REJECT, out.event(0).kind());
        assertEquals(10L, out.event(0).size());
        assertEquals(10_000L, out.event(0).price(), "reject carries the budget for FOK-BUDGET");
        assertEquals(1, book.orderCount());
    }

    @Test
    void fokBudgetFillsWhenBudgetSatisfied() {
        placeGtc(1L, true, 100L, 10L, MAKER);

        out.reset(0L, 0L);
        final long filled = book.matchFokBudget(2L, false, 600L, 6L, HIGH_RESERVE, TAKER, SPEC, out);

        assertEquals(6L, filled);
        assertEquals(1, out.eventCount());
        assertEquals(EventKind.TRADE, out.event(0).kind());
    }

    @Test
    void askFokBudgetFillsAgainstAnyBidPriceWhenProceedsCoverBudget() {
        placeGtc(1L, false, 100L, 10L, MAKER);

        out.reset(0L, 0L);
        // Budget 500 is a minimum-proceeds bound; the walk costs 10 * 100.
        final long filled = book.matchFokBudget(2L, true, 500L, 10L, 0L, TAKER, SPEC, out);

        assertEquals(10L, filled);
        assertEquals(1, out.eventCount());
        assertEquals(EventKind.TRADE, out.event(0).kind());
    }

    @Test
    void askFokBudgetKilledWhenProceedsCannotCoverTakerFee() {
        placeGtc(1L, false, 10L, 10L, MAKER);

        // Walked proceeds 10 * 10 = 100 cannot cover takerFee 100 * size 10.
        final SymbolSpec fees = new SymbolSpec(SYM, 0, 1, 1L, 1L, 100L, 0L);
        out.reset(0L, 0L);
        final long filled = book.matchFokBudget(2L, true, 1L, 10L, 0L, TAKER, fees, out);

        assertEquals(OrderBookNaive.FOK_KILLED_FEE_FLOOR, filled);
        assertEquals(1, out.eventCount());
        assertEquals(EventKind.REJECT, out.event(0).kind());
        assertEquals(1, book.orderCount(), "the kill must not mutate the book");
    }

    @Test
    void askFokBudgetKilledWhenScaledProceedsOverflow() {
        placeGtc(1L, false, 930_000_000_000L, 5L, MAKER);
        placeGtc(2L, false, 930_000_000_000L, 5L, MAKER);

        // Walked proceeds 10 * 9.3e11 fit, but * quoteScaleK 1e6 exceeds long.
        final SymbolSpec scaled = new SymbolSpec(SYM, 0, 1, 1L, 1_000_000L, 0L, 0L);
        out.reset(0L, 0L);
        final long filled = book.matchFokBudget(3L, true, 1L, 10L, 0L, TAKER, scaled, out);

        assertEquals(OrderBookNaive.FOK_KILLED_OVERFLOW, filled);
        assertEquals(1, out.eventCount());
        assertEquals(EventKind.REJECT, out.event(0).kind());
        assertEquals(2, book.orderCount(), "the kill must not mutate the book");
    }

    @Test
    void askFokBudgetKilledWhenWalkedCostItselfOverflows() {
        // Three bids at 4e18: the price-unit cost accumulation overflows before
        // quote scaling is even applied.
        placeGtc(1L, false, 4_000_000_000_000_000_000L, 1L, MAKER);
        placeGtc(2L, false, 4_000_000_000_000_000_000L, 1L, MAKER);
        placeGtc(3L, false, 4_000_000_000_000_000_000L, 1L, MAKER);

        out.reset(0L, 0L);
        final long filled = book.matchFokBudget(4L, true, 1L, 3L, 0L, TAKER, SPEC, out);

        assertEquals(OrderBookNaive.FOK_KILLED_OVERFLOW, filled);
        assertEquals(1, out.eventCount());
        assertEquals(EventKind.REJECT, out.event(0).kind());
        assertEquals(3, book.orderCount(), "the kill must not mutate the book");
    }

    @Test
    void cancelUnknownOrderReturnsUnknownId() {
        out.reset(0L, 0L);
        assertEquals(CommandResultCode.MATCHING_UNKNOWN_ORDER_ID, book.cancel(999L, MAKER, out));
    }

    @Test
    void cancelRemovesRestingOrderAndEmitsReduce() {
        placeGtc(1L, true, 100L, 10L, MAKER);

        out.reset(0L, 0L);
        final CommandResultCode code = book.cancel(1L, MAKER, out);

        assertEquals(CommandResultCode.SUCCESS, code);
        assertEquals(1, out.eventCount());
        assertEquals(EventKind.REDUCE, out.event(0).kind());
        assertEquals(10L, out.event(0).size());
        assertEquals(100L, out.event(0).price(), "reduce carries the resting price");
        assertTrue(out.event(0).makerCompleted(), "a cancel completes the order");
        assertEquals(0, book.orderCount());
    }

    @Test
    void reduceShrinksRestingOrder() {
        placeGtc(1L, true, 100L, 10L, MAKER);

        out.reset(0L, 0L);
        final CommandResultCode code = book.reduce(1L, MAKER, 4L, out);

        assertEquals(CommandResultCode.SUCCESS, code);
        assertEquals(EventKind.REDUCE, out.event(0).kind());
        assertEquals(4L, out.event(0).size());
        assertEquals(100L, out.event(0).price(), "reduce carries the resting price");
        assertFalse(out.event(0).makerCompleted(), "a partial reduce leaves the order resting");
        assertEquals(1, book.orderCount());
    }

    @Test
    void moveIntoMarketableRangeMatchesImmediately() {
        placeGtc(1L, true, 100L, 10L, MAKER);
        placeGtc(2L, false, 90L, 5L, TAKER);

        out.reset(0L, 0L);
        final CommandResultCode code = book.move(2L, TAKER, 100L, SPEC, out);

        assertEquals(CommandResultCode.SUCCESS, code);
        assertEquals(1, out.eventCount());
        assertEquals(EventKind.TRADE, out.event(0).kind());
        assertEquals(5L, out.event(0).size());
        assertEquals(1, book.orderCount());
    }

    @Test
    void moveBidAboveReserveIsRejected() {
        out.reset(0L, 0L);
        book.placeGtc(1L, false, 90L, 5L, 95L, TAKER, 0L, out);

        out.reset(0L, 0L);
        final CommandResultCode code = book.move(1L, TAKER, 100L, SPEC, out);

        assertEquals(CommandResultCode.MATCHING_MOVE_FAILED_PRICE_OVER_RISK_LIMIT, code);
    }

    @Test
    void l2ReflectsBookState() {
        placeGtc(1L, true, 100L, 10L, MAKER);
        placeGtc(2L, true, 101L, 3L, MAKER);
        placeGtc(3L, false, 99L, 5L, TAKER);

        final L2View l2 = new L2View(32);
        book.fillL2(l2);

        assertEquals(2, l2.askDepth());
        assertEquals(100L, l2.askPrice(0));
        assertEquals(10L, l2.askVolume(0));
        assertEquals(101L, l2.askPrice(1));
        assertEquals(1, l2.bidDepth());
        assertEquals(99L, l2.bidPrice(0));
        assertEquals(5L, l2.bidVolume(0));
    }

    private long placeGtc(final long orderId, final boolean ask, final long price, final long size, final long uid) {
        out.reset(0L, 0L);
        return book.placeGtc(orderId, ask, price, size, ask ? 0L : HIGH_RESERVE, uid, 0L, out);
    }
}
