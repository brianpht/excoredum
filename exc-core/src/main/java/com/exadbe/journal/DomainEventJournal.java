package com.exadbe.journal;

import com.exadbe.core.CommandOutcome;
import com.exadbe.protocol.JournalEventEncoder;
import com.exadbe.protocol.MatcherEventType;
import com.exadbe.protocol.MessageHeaderEncoder;
import com.exadbe.telemetry.CoreMetrics;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Encodes the matcher events produced by one command into fixed-size
 * {@link JournalEventEncoder} records and offers them to an {@link EventJournalRing}
 * at apply-time. Each record carries the idempotency key {@code (logPosition,
 * eventIndex)} so a consumer can dedup events re-emitted across a failover.
 *
 * <p>Single-writer: driven from the one service thread. Allocation-free in steady
 * state - a single scratch buffer and encoder are reused.
 *
 * <p>Events are never dropped: when the ring is full the producer idles until the
 * journaler agent drains a slot, keeping the audit stream gap-free. A full ring
 * therefore pauses command application, which is the intended trade - the
 * journaler runs on the same node and drains continuously, so stalls are brief
 * unless the recorder itself has failed (an observable error condition).
 */
public final class DomainEventJournal {

    private static final int SCRATCH_LENGTH = 128;

    private final EventJournalRing ring;
    private final CoreMetrics metrics;
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final JournalEventEncoder encoder = new JournalEventEncoder();
    private final UnsafeBuffer scratch = new UnsafeBuffer(new byte[SCRATCH_LENGTH]);

    public DomainEventJournal(final EventJournalRing ring, final CoreMetrics metrics) {
        this.ring = ring;
        this.metrics = metrics;
    }

    /**
     * Emits every matcher event in {@code out} to the ring, tagged with
     * {@code logPosition} and its intra-command index. Blocks (idling via
     * {@code idle}) while the ring is full; never drops an event.
     */
    public void emit(final CommandOutcome out, final long logPosition, final long timestamp, final IdleStrategy idle) {
        final int n = out.eventCount();
        for (int i = 0; i < n; i++) {
            final int length = encode(out.event(i), logPosition, i, timestamp);
            if (ring.offer(scratch, 0, length)) {
                continue;
            }
            metrics.onJournalBackpressure();
            idle.reset();
            while (!ring.offer(scratch, 0, length)) {
                idle.idle();
            }
        }
    }

    private int encode(
            final CommandOutcome.EventRecord e, final long logPosition, final int eventIndex, final long timestamp) {
        encoder.wrapAndApplyHeader(scratch, 0, headerEncoder)
                .logPosition(logPosition)
                .eventIndex(eventIndex)
                .eventIndexExt(eventIndex)
                .timestamp(timestamp)
                .eventType(typeOf(e.kind()))
                .symbolId(e.symbolId())
                .makerOrderId(e.makerOrderId())
                .makerUid(e.makerUid())
                .takerUid(e.takerUid())
                .price(e.price())
                .size(e.size())
                .makerCompleted((short) (e.makerCompleted() ? 1 : 0));
        return MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    }

    private static MatcherEventType typeOf(final CommandOutcome.EventKind kind) {
        return switch (kind) {
            case TRADE -> MatcherEventType.TRADE;
            case REDUCE -> MatcherEventType.REDUCE;
            case REJECT -> MatcherEventType.REJECT;
        };
    }
}
