package com.exadbe.engine;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.config.CoreConfig;
import com.exadbe.core.CommandOutcome;
import com.exadbe.snapshot.SnapshotManager;
import com.exadbe.telemetry.CoreMetrics;
import com.exadbe.testkit.InMemorySnapshot;
import org.junit.jupiter.api.Test;

/** A truncated or corrupted snapshot must never be accepted as committed state. */
class SnapshotIntegrityTest {

    private static final long CLIENT = 7L;
    private static final int SYMBOL = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;

    private final Commands commands = new Commands();
    private final CommandOutcome out = new CommandOutcome();

    private MatchingEngine funded() {
        final MatchingEngine engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        long seq = 0L;
        engine.process(commands.addSymbol(CLIENT, seq, seq++, SYMBOL, BASE, QUOTE, 1L, 1L), 1000L, out);
        engine.process(commands.addUser(CLIENT, seq, seq++, 1L), 1000L, out);
        engine.process(commands.adjust(CLIENT, seq, seq++, 1L, QUOTE, 500_000L), 1000L, out);
        engine.process(commands.adjust(CLIENT, seq, seq++, 1L, BASE, 250L), 1000L, out);
        engine.process(commands.placeGtc(CLIENT, seq, seq++, SYMBOL, 100L, false, 100L, 10L, 100L, 1L), 1001L, out);
        return engine;
    }

    @Test
    void truncatedSnapshotIsNotComplete() {
        final InMemorySnapshot snapshot = new InMemorySnapshot();
        snapshot.writeFrom(new SnapshotManager(), funded(), 1L);

        final MatchingEngine restored = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        final SnapshotManager readManager = new SnapshotManager();
        snapshot.readIntoDroppingFooter(readManager, restored);

        assertFalse(readManager.loadComplete(), "a missing footer must leave the load incomplete");
    }

    @Test
    void corruptedBalanceFailsInvariant() {
        final InMemorySnapshot snapshot = new InMemorySnapshot();
        snapshot.writeFrom(new SnapshotManager(), funded(), 1L);
        snapshot.corruptFirstBalanceValue();

        final MatchingEngine restored = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        final SnapshotManager readManager = new SnapshotManager();
        snapshot.readInto(readManager, restored);

        assertTrue(readManager.loadComplete(), "footer still applied");
        assertFalse(readManager.verifyInvariant(), "checksum must not match after corruption");
    }

    @Test
    void corruptedDedupFieldFailsInvariant() {
        final InMemorySnapshot snapshot = new InMemorySnapshot();
        snapshot.writeFrom(new SnapshotManager(), funded(), 1L);
        snapshot.corruptFirstDedupUid();

        final MatchingEngine restored = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        final SnapshotManager readManager = new SnapshotManager();
        snapshot.readInto(readManager, restored);

        assertTrue(readManager.loadComplete(), "footer still applied");
        assertFalse(readManager.verifyInvariant(), "the checksum must cover dedup uid/orderId/filledSize");
    }
}
