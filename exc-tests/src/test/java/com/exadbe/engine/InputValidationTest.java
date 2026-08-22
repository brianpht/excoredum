package com.exadbe.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.config.CoreConfig;
import com.exadbe.core.CommandOutcome;
import com.exadbe.protocol.CommandEnvelopeDecoder;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.telemetry.CoreMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Input validation and money-math guards: non-positive operands, overflow
 * pre-checks, the balance sentinel, spec sanity, the reserved dedup sentinel
 * sequence, and self-trade conservation.
 */
class InputValidationTest {

    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long UID = 7L;

    private MatchingEngine engine;
    private CommandOutcome out;
    private Commands commands;
    private CoreMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new CoreMetrics();
        engine = new MatchingEngine(CoreConfig.defaults(), metrics);
        out = new CommandOutcome();
        commands = new Commands();
        engine.process(commands.addUser(1L, 1L, 1L, UID), 0L, out);
        engine.process(commands.addSymbol(1L, 2L, 2L, SYM, BASE, QUOTE, 1L, 1L), 0L, out);
        engine.process(commands.adjust(1L, 3L, 3L, UID, BASE, 1_000L), 0L, out);
        engine.process(commands.adjust(1L, 4L, 4L, UID, QUOTE, 1_000_000L), 0L, out);
    }

    @Test
    void negativeSizeBidIsRejectedWithoutMintingMoney() {
        final long quoteBefore = engine.balance(UID, QUOTE);

        engine.process(commands.placeGtc(1L, 5L, 5L, SYM, 1L, false, 100L, -1L, 100L, UID), 0L, out);

        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode());
        assertEquals(quoteBefore, engine.balance(UID, QUOTE), "a negative hold must never credit the balance");
        assertEquals(0, engine.orderCount());
    }

    @Test
    void negativeSizeAskIsRejected() {
        engine.process(commands.placeGtc(1L, 5L, 5L, SYM, 1L, true, 100L, -5L, 0L, UID), 0L, out);

        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode());
        assertEquals(1_000L, engine.balance(UID, BASE));
        assertEquals(0, engine.orderCount());
    }

    @Test
    void zeroAndNegativePricesAreRejected() {
        engine.process(commands.placeGtc(1L, 5L, 5L, SYM, 1L, true, 0L, 1L, 0L, UID), 0L, out);
        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode());

        engine.process(commands.placeGtc(1L, 6L, 6L, SYM, 2L, false, -10L, 1L, 100L, UID), 0L, out);
        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode());

        assertEquals(0, engine.orderCount());
    }

    @Test
    void zeroBudgetFokIsRejected() {
        engine.process(commands.placeFokBudget(1L, 5L, 5L, SYM, 1L, false, 0L, 1L, UID), 0L, out);

        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode());
        assertEquals(1_000_000L, engine.balance(UID, QUOTE));
    }

    @Test
    void overflowingBidHoldIsRejectedBeforeAnyMutation() {
        final long quoteBefore = engine.balance(UID, QUOTE);

        engine.process(commands.placeGtc(1L, 5L, 5L, SYM, 1L, false, 100L, 2L, Long.MAX_VALUE, UID), 0L, out);

        assertEquals(CommandResultCode.OVERFLOW, out.resultCode());
        assertEquals(quoteBefore, engine.balance(UID, QUOTE));
        assertEquals(0, engine.orderCount());
    }

    @Test
    void overflowingAskProceedsAreRejectedBeforeAnyMutation() {
        final long baseBefore = engine.balance(UID, BASE);
        final long quoteBefore = engine.balance(UID, QUOTE);

        engine.process(commands.placeGtc(1L, 5L, 5L, SYM, 1L, true, Long.MAX_VALUE, 2L, 0L, UID), 0L, out);

        assertEquals(CommandResultCode.OVERFLOW, out.resultCode());
        assertEquals(baseBefore, engine.balance(UID, BASE));
        assertEquals(quoteBefore, engine.balance(UID, QUOTE));
        assertEquals(0, engine.orderCount());
    }

    @Test
    void moveWithNonPositivePriceIsRejected() {
        engine.process(commands.placeGtc(1L, 5L, 5L, SYM, 1L, true, 100L, 5L, 0L, UID), 0L, out);

        engine.process(commands.move(1L, 6L, 6L, SYM, 1L, 0L, UID), 0L, out);

        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode());
        assertEquals(100L, engine.book(SYM).bestAsk(), "the order must rest untouched");
    }

    @Test
    void moveAskBelowFeeFloorIsRejected() {
        engine.process(commands.addSymbol(1L, 7L, 7L, 2, BASE, QUOTE, 1L, 1L, 10L, 0L), 0L, out);
        engine.process(commands.placeGtc(1L, 8L, 8L, 2, 1L, true, 100L, 5L, 0L, UID), 0L, out);
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());

        engine.process(commands.move(1L, 9L, 9L, 2, 1L, 5L, UID), 0L, out);

        assertEquals(CommandResultCode.RISK_ASK_PRICE_LOWER_THAN_FEE, out.resultCode());
        assertEquals(100L, engine.book(2).bestAsk(), "the order must rest untouched");
        assertTrue(engine.balance(UID, QUOTE) >= 0L, "no settlement may drive a balance negative");
    }

    @Test
    void moveAskWithOverflowingProceedsIsRejected() {
        engine.process(commands.addSymbol(1L, 7L, 7L, 3, BASE, QUOTE, 2L, 1L), 0L, out);
        engine.process(commands.placeGtc(1L, 8L, 8L, 3, 1L, true, 100L, 5L, 0L, UID), 0L, out);

        engine.process(commands.move(1L, 9L, 9L, 3, 1L, Long.MAX_VALUE, UID), 0L, out);

        assertEquals(CommandResultCode.OVERFLOW, out.resultCode());
        assertEquals(100L, engine.book(3).bestAsk(), "the order must rest untouched");
    }

    @Test
    void balanceEqualToMissingSentinelIsRejected() {
        // Currency 99 has no balance yet, so delta MIN_VALUE lands exactly on
        // the absent-value sentinel.
        engine.process(commands.adjust(1L, 5L, 5L, UID, 99, Long.MIN_VALUE), 0L, out);

        assertEquals(CommandResultCode.OVERFLOW, out.resultCode());
        assertEquals(0L, engine.balance(UID, 99), "the adjustment must leave the balance intact");
        assertTrue(engine.userExists(UID));
    }

    @Test
    void invalidSymbolSpecsAreRejected() {
        engine.process(commands.addSymbol(1L, 10L, 10L, 4, BASE, QUOTE, 0L, 1L, 0L, 0L), 0L, out);
        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode(), "zero scale factor");

        engine.process(commands.addSymbol(1L, 11L, 11L, 5, BASE, QUOTE, 1L, 1L, -5L, 0L), 0L, out);
        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode(), "negative fee");

        engine.process(commands.addSymbol(1L, 12L, 12L, 6, BASE, QUOTE, 1L, 1L, 5L, 6L), 0L, out);
        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode(), "maker fee above taker fee");

        engine.process(commands.addSymbol(1L, 13L, 13L, 7, BASE, BASE, 1L, 1L, 0L, 0L), 0L, out);
        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode(), "base and quote must differ");

        assertEquals(1, engine.symbolCount(), "no invalid spec may be registered");
    }

    @Test
    void emptySentinelClientSeqIsRejectedAndNeverCached() {
        final long seq = com.exadbe.collections.DedupRing.EMPTY;

        engine.process(commands.placeGtc(1L, seq, 5L, SYM, 1L, true, 100L, 1L, 0L, UID), 0L, out);
        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode());

        // A retry of the sentinel seq is rejected again (never cached as a duplicate).
        final boolean duplicate =
                engine.process(commands.placeGtc(1L, seq, 6L, SYM, 2L, true, 100L, 1L, 0L, UID), 0L, out);
        assertFalse(duplicate);
        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode());

        assertEquals(0, engine.orderCount());
        assertEquals(1_000L, engine.balance(UID, BASE));
    }

    @Test
    void selfTradeIsAllowedAndConservesValue() {
        final long baseBefore = engine.balance(UID, BASE);
        final long quoteBefore = engine.balance(UID, QUOTE);

        engine.process(commands.placeGtc(1L, 5L, 5L, SYM, 1L, true, 100L, 5L, 0L, UID), 0L, out);
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        engine.process(commands.placeGtc(1L, 6L, 6L, SYM, 2L, false, 100L, 5L, 100L, UID), 0L, out);

        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        assertEquals(5L, out.filledSize(), "one user's bid matches their own ask");
        assertEquals(0, engine.orderCount());
        assertEquals(baseBefore, engine.balance(UID, BASE), "self-trade conserves base");
        assertEquals(quoteBefore, engine.balance(UID, QUOTE), "self-trade conserves quote at zero fees");
        assertFalse(engine.userExists(0L), "no fees accrue at zero fee rates");
    }

    @Test
    void uidCollidingWithUint64AbsentSentinelIsRejected() {
        // -1 is the uint64 "absent" sentinel of CommandResult/DedupRecord; a real
        // user with that id would be read back as "no uid" after a snapshot.
        engine.process(commands.addUser(1L, 5L, 5L, com.exadbe.collections.DedupRing.EMPTY), 0L, out);

        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode());
        assertFalse(engine.userExists(com.exadbe.collections.DedupRing.EMPTY));
    }

    @Test
    void uidCollidingWithStatusMapSentinelIsRejected() {
        // Long.MIN_VALUE is the account-status map's missing sentinel; storing a
        // user there would be rejected by the map (or read back as active).
        engine.process(commands.addUser(1L, 5L, 5L, com.exadbe.collections.AccountStore.MISSING), 0L, out);

        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode());
        assertFalse(engine.userExists(com.exadbe.collections.AccountStore.MISSING));
    }

    @Test
    void orderIdCollidingWithInt64AbsentSentinelIsRejected() {
        final long orderId = CommandEnvelopeDecoder.orderIdNullValue();

        engine.process(commands.placeGtc(1L, 5L, 5L, SYM, orderId, true, 100L, 1L, 0L, UID), 0L, out);

        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode());
        assertEquals(0, engine.orderCount(), "a sentinel order id must never rest");
        assertEquals(1_000L, engine.balance(UID, BASE));
    }

    @Test
    void scaleFactorAboveUpperBoundIsRejected() {
        engine.process(
                commands.addSymbol(
                        1L,
                        10L,
                        10L,
                        4,
                        BASE,
                        QUOTE,
                        com.exadbe.engine.handlers.AddSymbolHandler.MAX_SCALE_K + 1L,
                        1L,
                        0L,
                        0L),
                0L,
                out);

        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode());
        assertEquals(1, engine.symbolCount(), "an out-of-range scale factor must not register");
    }

    @Test
    void feeAboveUpperBoundIsRejected() {
        engine.process(
                commands.addSymbol(
                        1L,
                        10L,
                        10L,
                        4,
                        BASE,
                        QUOTE,
                        1L,
                        1L,
                        com.exadbe.engine.handlers.AddSymbolHandler.MAX_FEE + 1L,
                        0L),
                0L,
                out);

        assertEquals(CommandResultCode.INVALID_AMOUNT, out.resultCode());
        assertEquals(1, engine.symbolCount(), "an out-of-range fee must not register");
    }

    @Test
    void dedupEvictionIsCounted() {
        final CoreMetrics smallMetrics = new CoreMetrics();
        final MatchingEngine smallEngine =
                new MatchingEngine(CoreConfig.builder().dedupWindow(4).build(), smallMetrics);
        final CommandOutcome smallOut = new CommandOutcome();

        // Five distinct sequences for one client into a four-slot window: the
        // fifth evicts the first.
        for (long seq = 1L; seq <= 5L; seq++) {
            smallEngine.process(commands.addUser(1L, seq, seq, UID + seq), 0L, smallOut);
        }

        assertEquals(1L, smallMetrics.dedupEvictions());
    }
}
