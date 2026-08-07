package com.exadbe.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.exadbe.config.CoreConfig;
import com.exadbe.core.CommandOutcome;
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

    private void run(final com.exadbe.protocol.CommandEnvelopeDecoder decoded) {
        engine.process(decoded, 0L, out);
        seq++;
    }
}
