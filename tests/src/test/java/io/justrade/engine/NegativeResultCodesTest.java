package io.justrade.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.justrade.config.CoreConfig;
import io.justrade.core.CommandOutcome;
import io.justrade.protocol.CommandResultCode;
import io.justrade.protocol.OrderCommandType;
import io.justrade.telemetry.CoreMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Negative result-code paths: unknown symbol/command, wrong-uid mutations, reduce, RESET. */
class NegativeResultCodesTest {

    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long UID = 7L;

    private MatchingEngine engine;
    private CommandOutcome out;
    private Commands commands;

    @BeforeEach
    void setUp() {
        engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        out = new CommandOutcome();
        commands = new Commands();
        engine.process(commands.addUser(1L, 1L, 1L, UID), 0L, out);
        engine.process(commands.addSymbol(1L, 2L, 2L, SYM, BASE, QUOTE, 1L, 1L), 0L, out);
        engine.process(commands.adjust(1L, 3L, 3L, UID, BASE, 1_000L), 0L, out);
        engine.process(commands.adjust(1L, 4L, 4L, UID, QUOTE, 1_000_000L), 0L, out);
    }

    @Test
    void unknownSymbolIsRejected() {
        engine.process(commands.placeGtc(1L, 5L, 5L, 999, 1L, true, 100L, 1L, 0L, UID), 0L, out);

        assertEquals(CommandResultCode.INVALID_SYMBOL, out.resultCode());
        assertEquals(0, engine.orderCount());
    }

    @Test
    void unknownCommandTypeIsUnsupported() {
        engine.process(commands.encode(1L, 5L, 5L, OrderCommandType.NULL_VAL, UID, 0, 0L), 0L, out);

        assertEquals(CommandResultCode.UNSUPPORTED_COMMAND, out.resultCode());
    }

    @Test
    void reduceWithNonPositiveSizeIsRejected() {
        engine.process(commands.placeGtc(1L, 5L, 5L, SYM, 1L, true, 100L, 5L, 0L, UID), 0L, out);
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());

        engine.process(commands.reduce(1L, 6L, 6L, SYM, 1L, 0L, UID), 0L, out);

        assertEquals(CommandResultCode.MATCHING_REDUCE_FAILED_WRONG_SIZE, out.resultCode());
        assertEquals(1, engine.orderCount(), "the order must rest untouched");
    }

    @Test
    void wrongUidCannotCancel() {
        engine.process(commands.placeGtc(1L, 5L, 5L, SYM, 1L, true, 100L, 5L, 0L, UID), 0L, out);

        engine.process(commands.cancel(1L, 6L, 6L, SYM, 1L, UID + 1L), 0L, out);

        assertEquals(CommandResultCode.MATCHING_UNKNOWN_ORDER_ID, out.resultCode());
        assertEquals(1, engine.orderCount());
    }

    @Test
    void wrongUidCannotMove() {
        engine.process(commands.placeGtc(1L, 5L, 5L, SYM, 1L, true, 100L, 5L, 0L, UID), 0L, out);

        engine.process(commands.move(1L, 6L, 6L, SYM, 1L, 110L, UID + 1L), 0L, out);

        assertEquals(CommandResultCode.MATCHING_UNKNOWN_ORDER_ID, out.resultCode());
        assertEquals(100L, engine.book(SYM).bestAsk(), "the order must rest untouched");
    }

    @Test
    void wrongUidCannotReduce() {
        engine.process(commands.placeGtc(1L, 5L, 5L, SYM, 1L, true, 100L, 5L, 0L, UID), 0L, out);

        engine.process(commands.reduce(1L, 6L, 6L, SYM, 1L, 2L, UID + 1L), 0L, out);

        assertEquals(CommandResultCode.MATCHING_UNKNOWN_ORDER_ID, out.resultCode());
        assertEquals(1, engine.orderCount(), "the order must rest untouched");
    }

    @Test
    void resetClearsAllState() {
        engine.process(commands.placeGtc(1L, 5L, 5L, SYM, 1L, true, 100L, 5L, 0L, UID), 0L, out);
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        assertEquals(1, engine.orderCount());

        engine.process(commands.encode(1L, 6L, 6L, OrderCommandType.RESET, UID, 0, 0L), 0L, out);

        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        assertEquals(0, engine.orderCount());
        assertEquals(0, engine.userCount());
        assertEquals(0, engine.symbolCount());
    }
}
