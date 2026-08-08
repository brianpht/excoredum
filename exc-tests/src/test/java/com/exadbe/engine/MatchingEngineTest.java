package com.exadbe.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.config.CoreConfig;
import com.exadbe.core.CommandOutcome;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.telemetry.CoreMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Pure-engine unit tests for phase-1 account handlers and idempotency. */
class MatchingEngineTest {

    private static final int USD = 840;

    private MatchingEngine engine;
    private CommandOutcome out;
    private Commands commands;

    @BeforeEach
    void setUp() {
        engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        out = new CommandOutcome();
        commands = new Commands();
    }

    @Test
    void addUserCreatesAccount() {
        final boolean duplicate = engine.process(commands.addUser(1L, 1L, 100L, 7L), 0L, out);

        assertFalse(duplicate);
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        assertEquals(7L, out.uid());
        assertTrue(engine.userExists(7L));
        assertEquals(1, engine.userCount());
    }

    @Test
    void secondAddUserForSameUidRejected() {
        engine.process(commands.addUser(1L, 1L, 100L, 7L), 0L, out);

        engine.process(commands.addUser(1L, 2L, 101L, 7L), 0L, out);

        assertEquals(CommandResultCode.USER_ALREADY_EXISTS, out.resultCode());
        assertEquals(1, engine.userCount());
    }

    @Test
    void balanceAdjustmentAppliesSignedDelta() {
        engine.process(commands.addUser(1L, 1L, 100L, 7L), 0L, out);

        engine.process(commands.adjust(1L, 2L, 101L, 7L, USD, 500L), 0L, out);
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        assertEquals(500L, engine.balance(7L, USD));

        engine.process(commands.adjust(1L, 3L, 102L, 7L, USD, -200L), 0L, out);
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        assertEquals(300L, engine.balance(7L, USD));
    }

    @Test
    void adjustmentOnUnknownUserRejected() {
        engine.process(commands.adjust(1L, 1L, 100L, 99L, USD, 500L), 0L, out);

        assertEquals(CommandResultCode.USER_NOT_FOUND, out.resultCode());
        assertFalse(engine.userExists(99L));
    }

    @Test
    void suspendBlocksPlacementUntilResumed() {
        final int sym = 1;
        final int btc = 1;
        engine.process(commands.addUser(1L, 1L, 100L, 7L), 0L, out);
        engine.process(commands.addSymbol(1L, 2L, 101L, sym, btc, USD, 1L, 1L), 0L, out);
        engine.process(commands.adjust(1L, 3L, 102L, 7L, USD, 100_000L), 0L, out);

        engine.process(commands.suspend(1L, 4L, 103L, 7L), 0L, out);
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());

        engine.process(commands.placeGtc(1L, 5L, 104L, sym, 1L, false, 100L, 1L, 100L, 7L), 0L, out);
        assertEquals(CommandResultCode.USER_SUSPENDED, out.resultCode());

        engine.process(commands.resume(1L, 6L, 105L, 7L), 0L, out);
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());

        engine.process(commands.placeGtc(1L, 7L, 106L, sym, 2L, false, 100L, 1L, 100L, 7L), 0L, out);
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
    }

    @Test
    void suspendUnknownUserRejected() {
        engine.process(commands.suspend(1L, 1L, 100L, 99L), 0L, out);

        assertEquals(CommandResultCode.USER_NOT_FOUND, out.resultCode());
    }

    @Test
    void doubleSuspendRejected() {
        engine.process(commands.addUser(1L, 1L, 100L, 7L), 0L, out);
        engine.process(commands.suspend(1L, 2L, 101L, 7L), 0L, out);

        engine.process(commands.suspend(1L, 3L, 102L, 7L), 0L, out);

        assertEquals(CommandResultCode.USER_ALREADY_SUSPENDED, out.resultCode());
    }

    @Test
    void resumeNonSuspendedRejected() {
        engine.process(commands.addUser(1L, 1L, 100L, 7L), 0L, out);

        engine.process(commands.resume(1L, 2L, 101L, 7L), 0L, out);

        assertEquals(CommandResultCode.USER_NOT_SUSPENDED, out.resultCode());
    }

    @Test
    void overflowReturnsCodeAndLeavesBalanceUnchanged() {
        engine.process(commands.addUser(1L, 1L, 100L, 7L), 0L, out);
        engine.process(commands.adjust(1L, 2L, 101L, 7L, USD, Long.MAX_VALUE), 0L, out);
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());

        engine.process(commands.adjust(1L, 3L, 102L, 7L, USD, 1L), 0L, out);

        assertEquals(CommandResultCode.OVERFLOW, out.resultCode());
        assertEquals(Long.MAX_VALUE, engine.balance(7L, USD));
    }

    @Test
    void duplicateCommandReturnsCachedResultWithoutReapplying() {
        engine.process(commands.addUser(1L, 1L, 100L, 7L), 0L, out);
        engine.process(commands.adjust(1L, 2L, 101L, 7L, USD, 100L), 0L, out);
        assertEquals(100L, engine.balance(7L, USD));

        // Same (clientId, clientSeq) as the prior adjustment: must be a no-op apply.
        final boolean duplicate = engine.process(commands.adjust(1L, 2L, 101L, 7L, USD, 100L), 0L, out);

        assertTrue(duplicate);
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        assertEquals(100L, engine.balance(7L, USD));
    }
}
