package com.exadbe.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.exadbe.config.CoreConfig;
import com.exadbe.core.CommandOutcome;
import com.exadbe.engine.risk.DirectExchangeRisk;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.telemetry.CoreMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Direct-exchange (spot) risk: reserve, settle at fill price, release, NSF, FOK. */
class SpotRiskTest {

    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 1L;
    private static final long TAKER = 2L;

    private MatchingEngine engine;
    private CommandOutcome out;
    private Commands cmd;
    private long seq;

    @BeforeEach
    void setUp() {
        engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        out = new CommandOutcome();
        cmd = new Commands();
        seq = 1L;
        // Scale factors 1 keep the arithmetic readable.
        run(cmd.addSymbol(1L, seq, seq, SYM, BASE, QUOTE, 1L, 1L));
    }

    @Test
    void settlesMakerAndTakerAtActualFillPrice() {
        fund(MAKER, BASE, 1000L);
        fund(TAKER, QUOTE, 100_000L);
        placeGtc(1L, true, 100L, 10L, 0L, MAKER);

        placeGtc(2L, false, 100L, 6L, 100L, TAKER);
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        assertEquals(6L, out.filledSize());

        assertEquals(990L, engine.balance(MAKER, BASE)); // 10 reserved for the ask
        assertEquals(600L, engine.balance(MAKER, QUOTE)); // proceeds 6 * 100
        assertEquals(6L, engine.balance(TAKER, BASE));
        assertEquals(99_400L, engine.balance(TAKER, QUOTE)); // 100000 - 600 held
    }

    @Test
    void releasesOverReserveWhenFillBetterThanReservePrice() {
        fund(MAKER, BASE, 1000L);
        fund(TAKER, QUOTE, 100_000L);
        placeGtc(1L, true, 100L, 6L, 0L, MAKER);

        // Reserve at 120 but fill at 100: the extra (120-100)*6 must be released.
        placeGtc(2L, false, 100L, 6L, 120L, TAKER);

        assertEquals(6L, engine.balance(TAKER, BASE));
        assertEquals(99_400L, engine.balance(TAKER, QUOTE));
        assertEquals(600L, engine.balance(MAKER, QUOTE));
    }

    @Test
    void insufficientFundsLeavesBookUntouched() {
        fund(MAKER, BASE, 1000L);
        fund(TAKER, QUOTE, 100L); // far too little for 6 * 100
        placeGtc(1L, true, 100L, 10L, 0L, MAKER);

        placeGtc(2L, false, 100L, 6L, 100L, TAKER);

        assertEquals(CommandResultCode.RISK_NSF, out.resultCode());
        assertEquals(100L, engine.balance(TAKER, QUOTE)); // hold reverted
        assertEquals(1, engine.book(SYM).orderCount()); // only the maker ask
    }

    @Test
    void invalidReservePriceRejected() {
        fund(TAKER, QUOTE, 100_000L);
        // reserveBidPrice (90) below price (100) is invalid.
        run(cmd.placeGtc(1L, seq, seq, SYM, 2L, false, 100L, 6L, 90L, TAKER));
        assertEquals(CommandResultCode.RISK_INVALID_RESERVE_PRICE, out.resultCode());
    }

    @Test
    void iocBidFillsAndSettlesAtLimitPrice() {
        fund(MAKER, BASE, 1000L);
        fund(TAKER, QUOTE, 100_000L);
        placeGtc(1L, true, 100L, 6L, 0L, MAKER);

        // An IOC bid reserves quote at its limit price, then settles at the
        // fill price; the full resting ask is consumed.
        placeIoc(2L, false, 100L, 6L, TAKER);
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        assertEquals(6L, out.filledSize());

        assertEquals(6L, engine.balance(TAKER, BASE));
        assertEquals(99_400L, engine.balance(TAKER, QUOTE)); // 100000 - 6 * 100
        assertEquals(600L, engine.balance(MAKER, QUOTE));
    }

    @Test
    void fokBudgetFillsAndSettles() {
        fund(MAKER, BASE, 1000L);
        fund(TAKER, QUOTE, 100_000L);
        placeGtc(1L, true, 100L, 10L, 0L, MAKER);

        // Budget 600 for 6 units; reserveBidPrice must equal the budget.
        run(cmd.placeFokBudget(1L, seq, seq, SYM, 2L, false, 600L, 6L, TAKER));

        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        assertEquals(6L, out.filledSize());
        assertEquals(6L, engine.balance(TAKER, BASE));
        assertEquals(99_400L, engine.balance(TAKER, QUOTE));
        assertEquals(600L, engine.balance(MAKER, QUOTE));
    }

    @Test
    void fokBudgetRejectReleasesHoldAndLeavesBookUntouched() {
        fund(MAKER, BASE, 1000L);
        fund(TAKER, QUOTE, 100_000L);
        placeGtc(1L, true, 100L, 5L, 0L, MAKER); // only 5 available

        run(cmd.placeFokBudget(1L, seq, seq, SYM, 2L, false, 5_000L, 20L, TAKER));

        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        assertEquals(0L, out.filledSize());
        assertEquals(100_000L, engine.balance(TAKER, QUOTE)); // full budget hold released
        assertEquals(1, engine.book(SYM).orderCount());
    }

    @Test
    void cancelReleasesRestingHold() {
        fund(TAKER, QUOTE, 100_000L);
        placeGtc(1L, false, 100L, 6L, 120L, TAKER); // hold 6 * 120 = 720
        assertEquals(99_280L, engine.balance(TAKER, QUOTE));

        run(cmd.cancel(1L, seq, seq, SYM, 1L, TAKER));

        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        assertEquals(100_000L, engine.balance(TAKER, QUOTE));
    }

    @Test
    void askFokBudgetFillsAndSettlesWithFees() {
        final int sym = 2;
        run(cmd.addSymbol(1L, seq, seq, sym, BASE, QUOTE, 1L, 1L, 2L, 1L));
        run(cmd.addUser(1L, seq, seq, MAKER));
        run(cmd.adjust(1L, seq, seq, MAKER, QUOTE, 10_000L));
        run(cmd.addUser(1L, seq, seq, TAKER));
        run(cmd.adjust(1L, seq, seq, TAKER, BASE, 100L));
        // Resting bid: holds 10 * (100 + takerFee 2) = 1020 quote.
        run(cmd.placeGtc(1L, seq, seq, sym, 1L, false, 100L, 10L, 100L, MAKER));

        // Ask FOK: budget 500 is the minimum proceeds; the walk costs 1000.
        run(cmd.placeFokBudget(1L, seq, seq, sym, 2L, true, 500L, 10L, TAKER));

        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        assertEquals(10L, out.filledSize());
        assertEquals(10L, engine.balance(MAKER, BASE));
        assertEquals(8_990L, engine.balance(MAKER, QUOTE)); // 10000 - 1020 hold + 10 refund
        assertEquals(90L, engine.balance(TAKER, BASE));
        assertEquals(980L, engine.balance(TAKER, QUOTE)); // proceeds 1000 - takerFee 2 * 10
        assertEquals(30L, engine.balance(DirectExchangeRisk.FEE_ACCOUNT_UID, QUOTE));
        assertEquals(
                10_000L,
                engine.balance(MAKER, QUOTE)
                        + engine.balance(TAKER, QUOTE)
                        + engine.balance(DirectExchangeRisk.FEE_ACCOUNT_UID, QUOTE));
        assertEquals(100L, engine.balance(MAKER, BASE) + engine.balance(TAKER, BASE));
    }

    @Test
    void askFokBudgetKilledWhenProceedsCannotCoverTotalTakerFee() {
        final int sym = 2;
        run(cmd.addSymbol(1L, seq, seq, sym, BASE, QUOTE, 1L, 1L, 100L, 0L));
        run(cmd.addUser(1L, seq, seq, MAKER));
        run(cmd.adjust(1L, seq, seq, MAKER, QUOTE, 10_000L));
        run(cmd.addUser(1L, seq, seq, TAKER));
        run(cmd.adjust(1L, seq, seq, TAKER, BASE, 100L));
        run(cmd.placeGtc(1L, seq, seq, sym, 1L, false, 10L, 10L, 10L, MAKER));

        // Walked proceeds 100 cannot cover takerFee 100 * size 10.
        run(cmd.placeFokBudget(1L, seq, seq, sym, 2L, true, 1L, 10L, TAKER));

        assertEquals(CommandResultCode.RISK_ASK_PRICE_LOWER_THAN_FEE, out.resultCode());
        assertEquals(0L, out.filledSize());
        assertEquals(100L, engine.balance(TAKER, BASE)); // ask hold released
        assertEquals(8_900L, engine.balance(MAKER, QUOTE)); // bid hold 1100 untouched
        assertEquals(1, engine.book(sym).orderCount());
    }

    @Test
    void askFokBudgetSmallBudgetFillsWhenWalkedProceedsCoverFees() {
        // The floor is exact, not budget-based: budget 50 is below the per-lot
        // takerFee 100, yet the walked proceeds 2000 cover the total fee 1000.
        final int sym = 2;
        run(cmd.addSymbol(1L, seq, seq, sym, BASE, QUOTE, 1L, 1L, 100L, 0L));
        run(cmd.addUser(1L, seq, seq, MAKER));
        run(cmd.adjust(1L, seq, seq, MAKER, QUOTE, 10_000L));
        run(cmd.addUser(1L, seq, seq, TAKER));
        run(cmd.adjust(1L, seq, seq, TAKER, BASE, 100L));
        run(cmd.placeGtc(1L, seq, seq, sym, 1L, false, 200L, 10L, 200L, MAKER));

        run(cmd.placeFokBudget(1L, seq, seq, sym, 2L, true, 50L, 10L, TAKER));

        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        assertEquals(10L, out.filledSize());
        assertEquals(8_000L, engine.balance(MAKER, QUOTE)); // 10000 - 3000 hold + 1000 refund
        assertEquals(1_000L, engine.balance(TAKER, QUOTE)); // proceeds 2000 - fee 1000
        assertEquals(1_000L, engine.balance(DirectExchangeRisk.FEE_ACCOUNT_UID, QUOTE));
        assertEquals(
                10_000L,
                engine.balance(MAKER, QUOTE)
                        + engine.balance(TAKER, QUOTE)
                        + engine.balance(DirectExchangeRisk.FEE_ACCOUNT_UID, QUOTE));
    }

    @Test
    void askFokBudgetOverflowKillKeepsBookAndHoldsIntact() {
        // quoteScaleK 1e6: each bid holds 5 * 9.3e11 * 1e6 = 4.65e18 (fits), but
        // the taker's walked proceeds 9.3e12 * 1e6 exceed the signed 64-bit
        // range, so the kill must fire BEFORE any book mutation.
        final int sym = 2;
        final long maker2 = 3L;
        final long bidHold = 4_650_000_000_000_000_000L;
        run(cmd.addSymbol(1L, seq, seq, sym, BASE, QUOTE, 1L, 1_000_000L, 0L, 0L));
        run(cmd.addUser(1L, seq, seq, MAKER));
        run(cmd.adjust(1L, seq, seq, MAKER, QUOTE, bidHold));
        run(cmd.addUser(1L, seq, seq, maker2));
        run(cmd.adjust(1L, seq, seq, maker2, QUOTE, bidHold));
        run(cmd.addUser(1L, seq, seq, TAKER));
        run(cmd.adjust(1L, seq, seq, TAKER, BASE, 10L));
        run(cmd.placeGtc(1L, seq, seq, sym, 1L, false, 930_000_000_000L, 5L, 930_000_000_000L, MAKER));
        run(cmd.placeGtc(1L, seq, seq, sym, 2L, false, 930_000_000_000L, 5L, 930_000_000_000L, maker2));

        run(cmd.placeFokBudget(1L, seq, seq, sym, 3L, true, 1L, 10L, TAKER));

        assertEquals(CommandResultCode.OVERFLOW, out.resultCode());
        assertEquals(0L, out.filledSize());
        assertEquals(10L, engine.balance(TAKER, BASE)); // ask hold released
        assertEquals(0L, engine.balance(MAKER, QUOTE)); // bid hold intact (fully reserved)
        assertEquals(0L, engine.balance(maker2, QUOTE));
        assertEquals(2, engine.book(sym).orderCount(), "the kill must not consume resting bids");
    }

    @Test
    void feeAggregateOverflowIsRejectedAtPlacement() {
        // Fees at the MAX_FEE cap: (takerFee + makerFee) * size overflows even
        // though takerFee * size alone fits, so the aggregate must be bounded
        // up front for every order type.
        final int sym = 2;
        final long maxFee = 1_000_000_000_000L;
        run(cmd.addSymbol(1L, seq, seq, sym, BASE, QUOTE, 1L, 1L, maxFee, maxFee));
        run(cmd.addUser(1L, seq, seq, TAKER));
        run(cmd.adjust(1L, seq, seq, TAKER, QUOTE, Long.MAX_VALUE));

        run(cmd.placeFokBudget(1L, seq, seq, sym, 1L, false, 1L, 5_000_000L, TAKER));

        assertEquals(CommandResultCode.OVERFLOW, out.resultCode());
        assertEquals(Long.MAX_VALUE, engine.balance(TAKER, QUOTE)); // nothing reserved
        assertEquals(0, engine.book(sym) == null ? 0 : engine.book(sym).orderCount());
    }

    @Test
    void conservationAcrossTradesAndCancel() {
        fund(MAKER, BASE, 1000L);
        fund(TAKER, QUOTE, 100_000L);
        placeGtc(1L, true, 100L, 10L, 0L, MAKER);
        placeGtc(2L, true, 110L, 5L, 0L, MAKER);

        placeGtc(3L, false, 110L, 12L, 110L, TAKER); // fills 10 @ 100 then 2 @ 110

        run(cmd.cancel(1L, seq, seq, SYM, 2L, MAKER)); // release the 3 left on the second ask

        // With no resting holds, funds are conserved across the two users.
        assertEquals(1000L, engine.balance(MAKER, BASE) + engine.balance(TAKER, BASE));
        assertEquals(100_000L, engine.balance(MAKER, QUOTE) + engine.balance(TAKER, QUOTE));
    }

    private void fund(final long uid, final int currency, final long amount) {
        run(cmd.addUser(1L, seq, seq, uid));
        run(cmd.adjust(1L, seq, seq, uid, currency, amount));
    }

    private void placeGtc(
            final long orderId,
            final boolean ask,
            final long price,
            final long size,
            final long reserveBidPrice,
            final long uid) {
        run(cmd.placeGtc(1L, seq, seq, SYM, orderId, ask, price, size, reserveBidPrice, uid));
    }

    private void placeIoc(final long orderId, final boolean ask, final long price, final long size, final long uid) {
        run(cmd.placeIoc(1L, seq, seq, SYM, orderId, ask, price, size, uid));
    }

    private void run(final com.exadbe.protocol.CommandEnvelopeDecoder decoded) {
        engine.process(decoded, 0L, out);
        seq++;
    }
}
