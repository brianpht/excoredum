package com.exadbe.engine;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.config.CoreConfig;
import com.exadbe.core.CommandOutcome;
import com.exadbe.snapshot.SnapshotManager;
import com.exadbe.telemetry.CoreMetrics;
import com.exadbe.testkit.InMemorySnapshot;
import org.junit.jupiter.api.Test;

/** Round-trip snapshot: write, restore, verify integrity and byte-identical re-serialisation. */
class SnapshotRoundTripTest {

    private static final long CLIENT = 42L;
    private static final int SYMBOL = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;

    private final Commands commands = new Commands();
    private final CommandOutcome out = new CommandOutcome();

    private MatchingEngine buildPopulatedEngine() {
        final MatchingEngine engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        long seq = 0L;
        engine.process(commands.addSymbol(CLIENT, seq, seq++, SYMBOL, BASE, QUOTE, 1L, 1L), 1000L, out);
        for (long uid = 1L; uid <= 4L; uid++) {
            engine.process(commands.addUser(CLIENT, seq, seq++, uid), 1000L, out);
            engine.process(commands.adjust(CLIENT, seq, seq++, uid, QUOTE, 1_000_000L), 1000L, out);
            engine.process(commands.adjust(CLIENT, seq, seq++, uid, BASE, 1_000L), 1000L, out);
        }
        // Resting orders that only partially cross, leaving depth on both sides.
        engine.process(commands.placeGtc(CLIENT, seq, seq++, SYMBOL, 100L, false, 100L, 10L, 100L, 1L), 1001L, out);
        engine.process(commands.placeGtc(CLIENT, seq, seq++, SYMBOL, 101L, false, 99L, 5L, 100L, 2L), 1002L, out);
        engine.process(commands.placeGtc(CLIENT, seq, seq++, SYMBOL, 200L, true, 105L, 8L, 0L, 3L), 1003L, out);
        engine.process(commands.placeGtc(CLIENT, seq, seq++, SYMBOL, 201L, true, 106L, 4L, 0L, 4L), 1004L, out);
        // Marketable ask fills 3 of resting bid 100, leaving a partially filled resting order.
        engine.process(commands.placeGtc(CLIENT, seq, seq++, SYMBOL, 202L, true, 100L, 3L, 0L, 3L), 1005L, out);
        // Reduce a resting bid, keeping it on the book.
        engine.process(commands.reduce(CLIENT, seq, seq++, SYMBOL, 101L, 2L, 2L), 1006L, out);
        return engine;
    }

    @Test
    void writeThenLoadReproducesByteIdenticalStateAndInvariant() {
        final MatchingEngine source = buildPopulatedEngine();

        final InMemorySnapshot snapshot = new InMemorySnapshot();
        snapshot.writeFrom(new SnapshotManager(), source, 987L);

        final MatchingEngine restored = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        final SnapshotManager readManager = new SnapshotManager();
        snapshot.readInto(readManager, restored);

        assertTrue(readManager.loadComplete(), "footer must be applied");
        assertTrue(readManager.verifyInvariant(), "restored state must reproduce the footer checksum");
        assertEquals(987L, readManager.loadedLogPosition());

        // Re-serialising the restored engine must produce identical bytes.
        final InMemorySnapshot reSerialized = new InMemorySnapshot();
        reSerialized.writeFrom(new SnapshotManager(), restored, 987L);
        assertArrayEquals(snapshot.toByteArray(), reSerialized.toByteArray());

        // Spot-check restored engine state matches the source.
        assertEquals(source.userCount(), restored.userCount());
        assertEquals(source.symbolCount(), restored.symbolCount());
        assertEquals(source.orderCount(), restored.orderCount());
        assertEquals(source.balance(1L, QUOTE), restored.balance(1L, QUOTE));
        assertEquals(source.balance(3L, BASE), restored.balance(3L, BASE));
        assertEquals(4, restored.orderCount());
    }
}
