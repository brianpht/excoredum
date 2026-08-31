package io.justrade.collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.protocol.CommandResultCode;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Test;

/** Dedup ring and table mechanics: window indexing, eviction, isolation, sentinel. */
class DedupRingTest {

    private static final int WINDOW = 4;

    private static DedupRing ring() {
        return new DedupRing(WINDOW);
    }

    private static void put(final DedupRing ring, final long seq, final int code) {
        ring.put(seq, 0L, seq, code, 0L, false, 0L, false, 0L, false);
    }

    @Test
    void storedSequenceIsContained() {
        final DedupRing ring = ring();
        put(ring, 42L, CommandResultCode.SUCCESS.value());

        assertTrue(ring.contains(42L));
        assertEquals(CommandResultCode.SUCCESS.value(), ring.resultCode(42L));
        assertFalse(ring.contains(41L));
    }

    @Test
    void windowWrapEvictsTheOldestSequence() {
        final DedupRing ring = ring();
        for (long seq = 0L; seq < WINDOW; seq++) {
            put(ring, seq, CommandResultCode.SUCCESS.value());
        }

        final boolean evicted =
                ring.put(WINDOW, 0L, WINDOW, CommandResultCode.SUCCESS.value(), 0L, false, 0L, false, 0L, false);

        assertTrue(evicted, "seq 0 shares its slot with seq WINDOW and is evicted");
        assertFalse(ring.contains(0L), "the evicted sequence is no longer deduplicated");
        assertTrue(ring.contains(WINDOW));
        for (long seq = 1L; seq < WINDOW; seq++) {
            assertTrue(ring.contains(seq), "sequences inside the window survive");
        }
    }

    @Test
    void emptySentinelCannotBeStoredOrDetected() {
        final DedupRing ring = ring();
        // Seq 3 shares the sentinel's slot for WINDOW = 4.
        put(ring, 3L, CommandResultCode.SUCCESS.value());

        final boolean evicted =
                ring.put(DedupRing.EMPTY, 0L, 99L, CommandResultCode.SUCCESS.value(), 0L, false, 0L, false, 0L, false);

        assertFalse(evicted);
        assertFalse(ring.contains(DedupRing.EMPTY));
        assertTrue(ring.contains(3L), "the sentinel must never erase a colliding sequence");
        assertEquals(CommandResultCode.SUCCESS.value(), ring.resultCode(3L));
    }

    @Test
    void tableIsolatesClientsWithIdenticalSequences() {
        final DedupTable table = new DedupTable(8, WINDOW);

        table.store(1L, 7L, 0L, 100L, CommandResultCode.SUCCESS.value(), 1L, true, 10L, true, 5L, true);
        table.store(2L, 7L, 0L, 200L, CommandResultCode.RISK_NSF.value(), 2L, true, 20L, true, 0L, false);

        assertEquals(CommandResultCode.SUCCESS.value(), table.ringFor(1L).resultCode(7L));
        assertEquals(CommandResultCode.RISK_NSF.value(), table.ringFor(2L).resultCode(7L));
        assertEquals(100L, table.ringFor(1L).commandIdLo(7L));
        assertEquals(200L, table.ringFor(2L).commandIdLo(7L));
        assertEquals(2, table.clientCount());
    }

    @Test
    void tableReportsEvictionAcrossTheWindow() {
        final DedupTable table = new DedupTable(8, WINDOW);
        for (long seq = 0L; seq < WINDOW; seq++) {
            assertFalse(
                    table.store(1L, seq, 0L, seq, CommandResultCode.SUCCESS.value(), 0L, false, 0L, false, 0L, false));
        }

        assertTrue(table.store(
                1L, WINDOW, 0L, WINDOW, CommandResultCode.SUCCESS.value(), 0L, false, 0L, false, 0L, false));
    }

    @Property
    void everySequenceWithinTheWindowStaysContained(
            @ForAll @LongRange(min = WINDOW, max = 10_000L) final long head,
            @ForAll @IntRange(min = 0, max = 3) final int lookback) {
        final DedupRing ring = new DedupRing(WINDOW);
        for (long seq = Math.max(0L, head - 32L); seq <= head; seq++) {
            put(ring, seq, CommandResultCode.SUCCESS.value());
        }

        assertTrue(ring.contains(head - lookback));
    }

    @Property
    void sequencesOutsideTheWindowAreNotContained(@ForAll @LongRange(min = WINDOW, max = 10_000L) final long head) {
        final DedupRing ring = ring();
        for (long seq = head - WINDOW + 1L; seq <= head; seq++) {
            put(ring, seq, CommandResultCode.SUCCESS.value());
        }

        assertTrue(ring.contains(head));
        assertFalse(ring.contains(head - WINDOW), "a full window behind the head is evicted");
    }
}
