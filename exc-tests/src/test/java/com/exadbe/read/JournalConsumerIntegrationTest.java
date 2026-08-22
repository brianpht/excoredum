package com.exadbe.read;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.exadbe.core.CommandOutcome;
import com.exadbe.journal.DomainEventJournal;
import com.exadbe.journal.EventJournalRing;
import com.exadbe.launcher.EventJournalRecorder;
import com.exadbe.protocol.MatcherEventType;
import com.exadbe.telemetry.CoreMetrics;
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

/** The journal consumer delivers each committed event once, dropping failover replays. */
@Tag("integration")
class JournalConsumerIntegrationTest {

    private static final String CHANNEL = "aeron:ipc";
    private static final int STREAM_ID = 2001;
    private static final int SYM = 7;
    private static final long LOG_POSITION = 7_000L;

    @Test
    @Timeout(60)
    void redeliveredEventsAreDedupedToExactlyOnce() {
        final MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
                ExclusivePublication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID);
                Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID)) {

            awaitConnected(publication, subscription);

            final EventJournalRing ring = new EventJournalRing(64, 128);
            final DomainEventJournal journal = new DomainEventJournal(ring, new CoreMetrics());
            final EventJournalRecorder recorder = new EventJournalRecorder(
                    ring,
                    publication,
                    () -> aeron.addExclusivePublication(CHANNEL, STREAM_ID),
                    () -> {},
                    new YieldingIdleStrategy(),
                    16);

            final List<Long> tradePrices = new ArrayList<>();
            final JournalConsumer consumer = new JournalConsumer(
                    subscription,
                    (logPos, idx, type, symbolId, makerOrderId, makerUid, takerUid, price, size, makerCompleted) -> {
                        if (type == MatcherEventType.TRADE) {
                            tradePrices.add(price);
                        }
                    });

            final CommandOutcome out = new CommandOutcome();
            out.reset(0L, 1L);
            out.addTrade(SYM, 100L, 11L, 22L, 500L, 3L, true, false, 0L, true, 600L);
            out.addReduce(SYM, 101L, 33L, 4L, true, 600L, 550L, false);
            out.addReject(SYM, 102L, 44L, 5L, 560L);

            // First delivery: three unique committed events.
            journal.emit(out, LOG_POSITION, 42L, new YieldingIdleStrategy());
            pump(recorder, consumer, 3L, () -> consumer.unique() >= 3L);
            assertEquals(3L, consumer.unique());
            assertEquals(1, tradePrices.size());
            assertEquals(500L, tradePrices.get(0));

            // A failover re-publishes the same committed events with identical keys.
            journal.emit(out, LOG_POSITION, 42L, new YieldingIdleStrategy());
            pump(recorder, consumer, 6L, () -> consumer.duplicates() >= 3L);
            assertEquals(3L, consumer.unique(), "dedup must keep exactly-once delivery");
            assertEquals(3L, consumer.duplicates());
            assertEquals(1, tradePrices.size());
        } finally {
            driver.close();
        }
    }

    private static void pump(
            final EventJournalRecorder recorder,
            final JournalConsumer consumer,
            final long publishedTarget,
            final java.util.function.BooleanSupplier consumedEnough) {
        final long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline
                && (recorder.published() < publishedTarget || !consumedEnough.getAsBoolean())) {
            recorder.doWork();
            consumer.poll(16);
        }
    }

    private static void awaitConnected(final ExclusivePublication publication, final Subscription subscription) {
        final long deadline = System.currentTimeMillis() + 10_000L;
        while ((!publication.isConnected() || !subscription.isConnected()) && System.currentTimeMillis() < deadline) {
            Thread.onSpinWait();
        }
    }
}
