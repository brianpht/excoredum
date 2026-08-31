package io.justrade.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.core.CommandOutcome;
import io.justrade.telemetry.AtomicCounterSink;
import io.justrade.telemetry.CoreMetrics;
import io.justrade.telemetry.CounterSink;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.Test;

/** Verifies the P5 hot-path hardening: pooled nodes, bounded event buffer, off-heap counters. */
class HotPathHardeningTest {

    private static final long CLIENT = 1L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long UID = 1L;

    @Test
    void orderPoolReusesNodesAcrossPlaceCancelCycles() {
        final MatchingEngine engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        final Commands c = new Commands();
        final CommandOutcome out = new CommandOutcome();
        long seq = 0L;
        engine.process(c.addSymbol(CLIENT, seq, seq++, SYM, BASE, QUOTE, 1L, 1L), 1L, out);
        engine.process(c.addUser(CLIENT, seq, seq++, UID), 1L, out);
        engine.process(c.adjust(CLIENT, seq, seq++, UID, QUOTE, 1_000_000_000L), 1L, out);

        for (int i = 0; i < 2000; i++) {
            final long orderId = 1000L + i;
            engine.process(c.placeGtc(CLIENT, seq, seq++, SYM, orderId, false, 50L, 10L, 50L, UID), 1L, out);
            engine.process(c.cancel(CLIENT, seq, seq++, SYM, orderId, UID), 1L, out);
        }
        // Only one order rests at a time, so the pool never grows beyond a couple nodes.
        assertTrue(
                engine.orderPoolAllocations() <= 4L,
                "pool should reuse nodes; allocations=" + engine.orderPoolAllocations());
    }

    @Test
    void priceBucketPoolReusesBucketsAcrossPlaceCancelCycles() {
        final MatchingEngine engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        final Commands c = new Commands();
        final CommandOutcome out = new CommandOutcome();
        long seq = 0L;
        engine.process(c.addSymbol(CLIENT, seq, seq++, SYM, BASE, QUOTE, 1L, 1L), 1L, out);
        engine.process(c.addUser(CLIENT, seq, seq++, UID), 1L, out);
        engine.process(c.adjust(CLIENT, seq, seq++, UID, QUOTE, 1_000_000_000L), 1L, out);

        for (int i = 0; i < 2000; i++) {
            final long orderId = 1000L + i;
            engine.process(c.placeGtc(CLIENT, seq, seq++, SYM, orderId, false, 50L, 10L, 50L, UID), 1L, out);
            engine.process(c.cancel(CLIENT, seq, seq++, SYM, orderId, UID), 1L, out);
        }
        // One price level comes and goes per cycle, so the pooled bucket is reused
        // and the pool allocates only a handful of buckets in total.
        assertTrue(
                engine.priceBucketPoolAllocations() <= 4L,
                "bucket pool should reuse levels; allocations=" + engine.priceBucketPoolAllocations());
    }

    @Test
    void commandOutcomeFlagsEventBufferGrowth() {
        final CommandOutcome out = new CommandOutcome(2);
        out.reset(0L, 0L);
        out.addReject(SYM, 1L, UID, 1L, 0L);
        out.addReject(SYM, 2L, UID, 1L, 0L);
        assertFalse(out.grewEventBuffer(), "two events fit the preallocated buffer");
        out.addReject(SYM, 3L, UID, 1L, 0L);
        assertTrue(out.grewEventBuffer(), "the third event overflowed and grew the buffer");
        out.reset(0L, 0L);
        assertFalse(out.grewEventBuffer(), "the flag clears on reset");
    }

    @Test
    void offHeapCounterSinkMirrorsIncrements() {
        final int counterCount = CounterSink.Counter.COUNT;
        final int gaugeCount = CounterSink.Gauge.COUNT;
        final int total = counterCount + gaugeCount;
        final UnsafeBuffer values = new UnsafeBuffer(
                BufferUtil.allocateDirectAligned(total * CountersManager.COUNTER_LENGTH, BitUtil.CACHE_LINE_LENGTH));
        final UnsafeBuffer meta = new UnsafeBuffer(
                BufferUtil.allocateDirectAligned(total * CountersManager.METADATA_LENGTH, BitUtil.CACHE_LINE_LENGTH));
        final CountersManager cm = new CountersManager(meta, values);

        final AtomicCounter[] counters = new AtomicCounter[counterCount];
        for (int i = 0; i < counterCount; i++) {
            counters[i] = cm.newCounter("c" + i, CounterSink.TYPE_COUNTER);
        }
        final AtomicCounter[] gauges = new AtomicCounter[gaugeCount];
        for (int i = 0; i < gaugeCount; i++) {
            gauges[i] = cm.newCounter("g" + i, CounterSink.TYPE_GAUGE);
        }

        final CoreMetrics metrics = new CoreMetrics(new AtomicCounterSink(counters, gauges));
        metrics.onCommandProcessed();
        metrics.onCommandProcessed();
        metrics.onDuplicate();
        metrics.snapshotWriteMillis(42L);

        assertEquals(2L, counters[CounterSink.Counter.COMMANDS_PROCESSED.ordinal()].get());
        assertEquals(1L, counters[CounterSink.Counter.DUPLICATES.ordinal()].get());
        assertEquals(42L, gauges[CounterSink.Gauge.SNAPSHOT_WRITE_MILLIS.ordinal()].get());
    }
}
