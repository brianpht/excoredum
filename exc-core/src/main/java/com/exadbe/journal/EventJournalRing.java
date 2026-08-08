package com.exadbe.journal;

import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Single-producer/single-consumer off-heap ring for domain events. The producer
 * (the service thread) offers fixed-slot records at apply-time; the consumer (the
 * journaler agent) drains them to durable storage on a separate thread.
 *
 * <p>Producer and consumer positions live on separate cache lines to avoid false
 * sharing, and cross-thread visibility uses acquire/release ordering. A full ring
 * makes {@link #offer} return {@code false} - the caller applies backpressure and
 * never drops.
 */
public final class EventJournalRing {

    /** Receives one event payload during {@link #poll}. */
    @FunctionalInterface
    public interface EventHandler {
        void onEvent(DirectBuffer buffer, int offset, int length);
    }

    private static final int LENGTH_HEADER = BitUtil.SIZE_OF_INT;
    private static final int PRODUCER_OFFSET = 0;
    private static final int CONSUMER_OFFSET = BitUtil.CACHE_LINE_LENGTH;

    private final int slotCount;
    private final int slotSize;
    private final int mask;
    private final int maxPayload;
    private final UnsafeBuffer control;
    private final UnsafeBuffer data;

    public EventJournalRing(final int slotCount, final int slotSize) {
        if (Integer.bitCount(slotCount) != 1) {
            throw new IllegalArgumentException("slotCount must be a power of two, was: " + slotCount);
        }
        if (slotSize <= LENGTH_HEADER) {
            throw new IllegalArgumentException("slotSize must exceed the length header, was: " + slotSize);
        }
        this.slotCount = slotCount;
        this.slotSize = slotSize;
        this.mask = slotCount - 1;
        this.maxPayload = slotSize - LENGTH_HEADER;
        this.control = new UnsafeBuffer(
                BufferUtil.allocateDirectAligned(2 * BitUtil.CACHE_LINE_LENGTH, BitUtil.CACHE_LINE_LENGTH));
        this.data = new UnsafeBuffer(BufferUtil.allocateDirectAligned(slotCount * slotSize, BitUtil.CACHE_LINE_LENGTH));
    }

    /** Largest event payload the ring can hold in one slot. */
    public int maxPayloadLength() {
        return maxPayload;
    }

    /**
     * Copies one event into the ring; single producer only.
     *
     * @return {@code false} if the ring is full (the caller must retry, not drop).
     */
    public boolean offer(final DirectBuffer src, final int offset, final int length) {
        if (length > maxPayload) {
            throw new IllegalArgumentException("event too large: " + length + " > " + maxPayload);
        }
        final long producer = control.getLong(PRODUCER_OFFSET);
        final long consumer = control.getLongAcquire(CONSUMER_OFFSET);
        if (producer - consumer >= slotCount) {
            return false;
        }
        final int slotOffset = ((int) producer & mask) * slotSize;
        data.putInt(slotOffset, length);
        data.putBytes(slotOffset + LENGTH_HEADER, src, offset, length);
        control.putLongRelease(PRODUCER_OFFSET, producer + 1L);
        return true;
    }

    /**
     * Delivers up to {@code limit} pending events to {@code handler}; single
     * consumer only.
     *
     * @return the number of events delivered.
     */
    public int poll(final EventHandler handler, final int limit) {
        final long consumer = control.getLong(CONSUMER_OFFSET);
        final long producer = control.getLongAcquire(PRODUCER_OFFSET);
        final int available = (int) Math.min(limit, producer - consumer);
        for (int i = 0; i < available; i++) {
            final int slotOffset = ((int) (consumer + i) & mask) * slotSize;
            final int length = data.getInt(slotOffset);
            handler.onEvent(data, slotOffset + LENGTH_HEADER, length);
        }
        if (available > 0) {
            control.putLongRelease(CONSUMER_OFFSET, consumer + available);
        }
        return available;
    }

    /** Total events offered so far (for tests and telemetry). */
    public long producerPosition() {
        return control.getLongAcquire(PRODUCER_OFFSET);
    }

    /** Total events consumed so far (for tests and telemetry). */
    public long consumerPosition() {
        return control.getLongAcquire(CONSUMER_OFFSET);
    }

    /** Number of slots (power of two). */
    public int slotCount() {
        return slotCount;
    }
}
