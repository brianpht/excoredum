package com.exadbe.journal;

import com.exadbe.core.CommandOutcome;
import com.exadbe.protocol.JournalEventEncoder;
import com.exadbe.protocol.MatcherEventType;
import com.exadbe.protocol.MessageHeaderEncoder;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Encodes the matcher events produced by one command into fixed-size
 * {@link JournalEventEncoder} records and offers them to an {@link EventJournalRing}
 * at apply-time. Each record carries the idempotency key {@code (logPosition,
 * eventIndex)} so a consumer can dedup events re-emitted across a failover.
 *
 * <p>Single-writer: driven from the one service thread. Allocation-free in steady
 * state - a single scratch buffer and encoder are reused.
 */
public final class DomainEventJournal {

    private static final int SCRATCH_LENGTH = 128;

    private final EventJournalRing ring;
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final JournalEventEncoder encoder = new JournalEventEncoder();
    private final UnsafeBuffer scratch = new UnsafeBuffer(new byte[SCRATCH_LENGTH]);

    private long droppedEvents;

    public DomainEventJournal(final EventJournalRing ring) {
        this.ring = ring;
    }

    /**
     * Emits every matcher event in {@code out} to the ring, tagged with
     * {@code logPosition} and its intra-command index.
     *
     * @return {@code true} if all events were accepted; {@code false} if the ring
     *     was full for at least one (the caller must apply backpressure).
     */
    public boolean emit(final CommandOutcome out, final long logPosition, final long timestamp) {
        final int n = out.eventCount();
        boolean allAccepted = true;
        for (int i = 0; i < n; i++) {
            final int length = encode(out.event(i), logPosition, i, timestamp);
            if (!ring.offer(scratch, 0, length)) {
                droppedEvents++;
                allAccepted = false;
            }
        }
        return allAccepted;
    }

    private int encode(
            final CommandOutcome.EventRecord e, final long logPosition, final int eventIndex, final long timestamp) {
        encoder.wrapAndApplyHeader(scratch, 0, headerEncoder)
                .logPosition(logPosition)
                .eventIndex(eventIndex)
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

    /** Cumulative events that could not be offered because the ring was full. */
    public long droppedEvents() {
        return droppedEvents;
    }
}
