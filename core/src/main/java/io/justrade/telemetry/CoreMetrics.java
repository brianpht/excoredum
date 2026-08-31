package io.justrade.telemetry;

import io.justrade.telemetry.CounterSink.Counter;
import io.justrade.telemetry.CounterSink.Gauge;

/**
 * Single-writer core counters. All mutation happens on the one service thread,
 * so plain long fields are sufficient and lock-free.
 *
 * <p>Each update is also mirrored to a {@link CounterSink}, letting the cluster
 * expose the same values off-heap for external readers without disturbing the
 * single-writer hot path. The default sink is a no-op, used by tests and the raw
 * engine. The hot path only increments counters - no string formatting, no
 * allocation.
 */
public final class CoreMetrics {

    private final CounterSink sink;

    private long commandsProcessed;
    private long duplicates;
    private long backpressureEvents;
    private long unsupportedCommands;
    private long snapshotsTaken;
    private long snapshotsLoaded;
    private long eventBufferOverflows;
    private long orderPoolExhaustions;
    private long priceBucketPoolExhaustions;
    private long journalBackpressureEvents;
    private long dedupEvictions;
    private long lastSnapshotWriteMillis;
    private long lastSnapshotReadMillis;

    /** Creates metrics that only maintain in-heap counters (tests, raw engine). */
    public CoreMetrics() {
        this(CounterSink.NOOP);
    }

    /** Creates metrics that also mirror counters to {@code sink}. */
    public CoreMetrics(final CounterSink sink) {
        this.sink = sink;
    }

    public void onCommandProcessed() {
        commandsProcessed++;
        sink.increment(Counter.COMMANDS_PROCESSED);
    }

    public void onDuplicate() {
        duplicates++;
        sink.increment(Counter.DUPLICATES);
    }

    public void onBackpressure() {
        backpressureEvents++;
        sink.increment(Counter.BACKPRESSURE);
    }

    public void onUnsupportedCommand() {
        unsupportedCommands++;
        sink.increment(Counter.UNSUPPORTED_COMMANDS);
    }

    public void onSnapshotTaken() {
        snapshotsTaken++;
        sink.increment(Counter.SNAPSHOTS_TAKEN);
    }

    public void onSnapshotLoaded() {
        snapshotsLoaded++;
        sink.increment(Counter.SNAPSHOTS_LOADED);
    }

    // A single command produced more matcher events than the preallocated buffer
    // held, forcing a cold-path grow. A rising count means the buffer is too small.
    public void onEventBufferOverflow() {
        eventBufferOverflows++;
        sink.increment(Counter.EVENT_BUFFER_OVERFLOW);
    }

    // The order pool was empty when a resting order needed a node, forcing a
    // cold-path allocation. A rising count means the pool is undersized.
    public void onOrderPoolExhausted() {
        orderPoolExhaustions++;
        sink.increment(Counter.ORDER_POOL_EXHAUSTED);
    }

    // The price-bucket pool was empty when a fresh price level was needed,
    // forcing a cold-path allocation. A rising count means the pool is undersized.
    public void onPriceBucketPoolExhausted() {
        priceBucketPoolExhaustions++;
        sink.increment(Counter.PRICE_BUCKET_POOL_EXHAUSTED);
    }

    // The journal ring was full when the service offered a domain event, so the
    // producer idled until the journaler drained a slot. Events are never
    // dropped; a rising count means the journaler cannot keep up.
    public void onJournalBackpressure() {
        journalBackpressureEvents++;
        sink.increment(Counter.JOURNAL_BACKPRESSURE);
    }

    // A command's dedup entry evicted a different, older sequence from a client's
    // bounded window. A live resend of that evicted sequence would now be applied
    // a second time; a rising count means a client's window is mis-sized.
    public void onDedupEviction() {
        dedupEvictions++;
        sink.increment(Counter.DEDUP_EVICTIONS);
    }

    public void snapshotWriteMillis(final long millis) {
        this.lastSnapshotWriteMillis = millis;
        sink.set(Gauge.SNAPSHOT_WRITE_MILLIS, millis);
    }

    public void snapshotReadMillis(final long millis) {
        this.lastSnapshotReadMillis = millis;
        sink.set(Gauge.SNAPSHOT_READ_MILLIS, millis);
    }

    public long commandsProcessed() {
        return commandsProcessed;
    }

    public long duplicates() {
        return duplicates;
    }

    public long backpressureEvents() {
        return backpressureEvents;
    }

    public long unsupportedCommands() {
        return unsupportedCommands;
    }

    public long snapshotsTaken() {
        return snapshotsTaken;
    }

    public long snapshotsLoaded() {
        return snapshotsLoaded;
    }

    public long eventBufferOverflows() {
        return eventBufferOverflows;
    }

    public long orderPoolExhaustions() {
        return orderPoolExhaustions;
    }

    public long priceBucketPoolExhaustions() {
        return priceBucketPoolExhaustions;
    }

    public long journalBackpressureEvents() {
        return journalBackpressureEvents;
    }

    public long dedupEvictions() {
        return dedupEvictions;
    }

    public long lastSnapshotWriteMillis() {
        return lastSnapshotWriteMillis;
    }

    public long lastSnapshotReadMillis() {
        return lastSnapshotReadMillis;
    }
}
