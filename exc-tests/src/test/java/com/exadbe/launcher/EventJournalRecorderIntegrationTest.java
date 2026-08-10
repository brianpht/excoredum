package com.exadbe.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.exadbe.core.CommandOutcome;
import com.exadbe.journal.DomainEventJournal;
import com.exadbe.journal.EventJournalRing;
import com.exadbe.protocol.JournalEventDecoder;
import com.exadbe.protocol.MatcherEventType;
import com.exadbe.protocol.MessageHeaderDecoder;
import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The recorder drains the off-heap ring onto an Aeron stream a subscriber can decode. */
@Tag("integration")
class EventJournalRecorderIntegrationTest {

    private static final String CHANNEL = "aeron:ipc";
    private static final int STREAM_ID = 1001;
    private static final int SYM = 7;

    @Test
    @Timeout(60)
    void drainsRingToAeronStream() {
        final MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
                ExclusivePublication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID);
                Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID)) {

            awaitConnected(publication, subscription);

            final EventJournalRing ring = new EventJournalRing(64, 128);
            final DomainEventJournal journal = new DomainEventJournal(ring);
            final EventJournalRecorder recorder =
                    new EventJournalRecorder(ring, publication, new YieldingIdleStrategy(), 16);

            final CommandOutcome out = new CommandOutcome();
            out.reset(0L, 1L);
            out.addTrade(SYM, 100L, 11L, 22L, 500L, 3L, true, false, 0L, true, 600L);
            out.addReduce(SYM, 101L, 33L, 4L, true, 600L, 550L, false);
            out.addReject(SYM, 102L, 44L, 5L, 560L);
            journal.emit(out, 7_000L, 42L);

            long drained = 0;
            final long drainDeadline = System.currentTimeMillis() + 5_000L;
            while (drained < 3 && System.currentTimeMillis() < drainDeadline) {
                drained += recorder.doWork();
            }
            assertEquals(3L, drained);

            final List<Decoded> received = poll(subscription, 3);
            assertEquals(3, received.size());
            assertEquals(MatcherEventType.TRADE, received.get(0).type);
            assertEquals(7_000L, received.get(0).logPosition);
            assertEquals(0, received.get(0).eventIndex);
            assertEquals(22L, received.get(0).takerUid);
            assertEquals(MatcherEventType.REDUCE, received.get(1).type);
            assertEquals(1, received.get(1).eventIndex);
            assertEquals(MatcherEventType.REJECT, received.get(2).type);
            assertEquals(2, received.get(2).eventIndex);
        } finally {
            driver.close();
        }
    }

    private static void awaitConnected(final ExclusivePublication publication, final Subscription subscription) {
        final long deadline = System.currentTimeMillis() + 10_000L;
        while ((!publication.isConnected() || !subscription.isConnected()) && System.currentTimeMillis() < deadline) {
            Thread.onSpinWait();
        }
    }

    private static List<Decoded> poll(final Subscription subscription, final int expected) {
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        final JournalEventDecoder decoder = new JournalEventDecoder();
        final List<Decoded> received = new ArrayList<>();
        final long deadline = System.currentTimeMillis() + 5_000L;
        while (received.size() < expected && System.currentTimeMillis() < deadline) {
            subscription.poll(
                    (buffer, offset, length, aeronHeader) -> {
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
                        d.takerUid = decoder.takerUid();
                        received.add(d);
                    },
                    10);
        }
        return received;
    }

    private static final class Decoded {
        private long logPosition;
        private int eventIndex;
        private MatcherEventType type;
        private long takerUid;
    }
}
