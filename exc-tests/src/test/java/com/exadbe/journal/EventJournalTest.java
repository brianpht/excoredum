package com.exadbe.journal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.core.CommandOutcome;
import com.exadbe.protocol.JournalEventDecoder;
import com.exadbe.protocol.MatcherEventType;
import com.exadbe.protocol.MessageHeaderDecoder;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;

/** Off-heap SPSC ring and domain-event encoding: ordering, backpressure, idempotency key. */
class EventJournalTest {

    private static final int SYM = 7;

    @Test
    void ringDeliversEventsInOrder() {
        final EventJournalRing ring = new EventJournalRing(8, 64);
        final org.agrona.concurrent.UnsafeBuffer src = new org.agrona.concurrent.UnsafeBuffer(new byte[8]);

        for (int i = 0; i < 5; i++) {
            src.putInt(0, i);
            assertTrue(ring.offer(src, 0, 4));
        }

        final int[] seen = {0};
        final int delivered = ring.poll(
                (buffer, offset, length) -> {
                    assertEquals(seen[0], buffer.getInt(offset));
                    seen[0]++;
                },
                10);

        assertEquals(5, delivered);
        assertEquals(5, ring.consumerPosition());
    }

    @Test
    void ringSignalsBackpressureWhenFull() {
        final EventJournalRing ring = new EventJournalRing(4, 64);
        final org.agrona.concurrent.UnsafeBuffer src = new org.agrona.concurrent.UnsafeBuffer(new byte[8]);
        src.putInt(0, 1);

        for (int i = 0; i < 4; i++) {
            assertTrue(ring.offer(src, 0, 4));
        }
        assertFalse(ring.offer(src, 0, 4), "a full ring must refuse rather than drop");

        assertEquals(1, ring.poll((b, o, l) -> {}, 1));
        assertTrue(ring.offer(src, 0, 4), "a freed slot must accept again");
    }

    @Test
    void journalEncodesEventsWithIdempotencyKey() {
        final EventJournalRing ring = new EventJournalRing(16, 128);
        final DomainEventJournal journal = new DomainEventJournal(ring);

        final CommandOutcome out = new CommandOutcome();
        out.reset(0L, 1L);
        out.addTrade(SYM, 100L, 11L, 22L, 500L, 3L, true, false, 0L, true, 600L);
        out.addReduce(SYM, 101L, 33L, 4L, true, 600L);
        out.addReject(SYM, 102L, 44L, 5L);

        final long logPosition = 9_000L;
        assertTrue(journal.emit(out, logPosition, 1234L));

        final List<Decoded> events = drain(ring);
        assertEquals(3, events.size());

        final Decoded trade = events.get(0);
        assertEquals(MatcherEventType.TRADE, trade.type);
        assertEquals(logPosition, trade.logPosition);
        assertEquals(0, trade.eventIndex);
        assertEquals(100L, trade.makerOrderId);
        assertEquals(11L, trade.makerUid);
        assertEquals(22L, trade.takerUid);
        assertEquals(500L, trade.price);
        assertEquals(3L, trade.size);

        assertEquals(MatcherEventType.REDUCE, events.get(1).type);
        assertEquals(1, events.get(1).eventIndex);
        assertEquals(101L, events.get(1).makerOrderId);

        assertEquals(MatcherEventType.REJECT, events.get(2).type);
        assertEquals(2, events.get(2).eventIndex);
        assertEquals(5L, events.get(2).size);
    }

    private static List<Decoded> drain(final EventJournalRing ring) {
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        final JournalEventDecoder decoder = new JournalEventDecoder();
        final List<Decoded> out = new ArrayList<>();
        ring.poll(
                (DirectBuffer buffer, int offset, int length) -> {
                    header.wrap(buffer, offset);
                    decoder.wrap(
                            buffer,
                            offset + MessageHeaderDecoder.ENCODED_LENGTH,
                            header.blockLength(),
                            header.version());
                    final Decoded d = new Decoded();
                    d.logPosition = decoder.logPosition();
                    d.eventIndex = decoder.eventIndex();
                    d.type = decoder.eventType();
                    d.makerOrderId = decoder.makerOrderId();
                    d.makerUid = decoder.makerUid();
                    d.takerUid = decoder.takerUid();
                    d.price = decoder.price();
                    d.size = decoder.size();
                    out.add(d);
                },
                64);
        return out;
    }

    private static final class Decoded {
        private long logPosition;
        private int eventIndex;
        private MatcherEventType type;
        private long makerOrderId;
        private long makerUid;
        private long takerUid;
        private long price;
        private long size;
    }
}
