package io.justrade.journal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.core.CommandOutcome;
import io.justrade.telemetry.CoreMetrics;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The journal producer never drops: a full ring blocks emission (idling) until
 * the journaler drains a slot, and the stall is counted for observability.
 */
class JournalBackpressureTest {

    @Test
    @Timeout(30)
    void emitBlocksOnFullRingInsteadOfDropping() throws Exception {
        final EventJournalRing ring = new EventJournalRing(2, 128);
        final CoreMetrics metrics = new CoreMetrics();
        final DomainEventJournal journal = new DomainEventJournal(ring, metrics);

        final CommandOutcome out = new CommandOutcome();
        out.reset(0L, 1L);
        for (int i = 0; i < 3; i++) {
            out.addTrade(1, 100L + i, 11L, 22L, 500L, 3L, true, false, 0L, true, 600L);
        }

        // The ring holds only two of the three events, so the third must wait
        // for the consumer. The drainer starts after a short delay to force
        // the producer into the blocking path.
        final Thread drainer = new Thread(() -> {
            try {
                Thread.sleep(200L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            final long deadline = System.currentTimeMillis() + 10_000L;
            while (ring.consumerPosition() < 3L && System.currentTimeMillis() < deadline) {
                ring.poll((buffer, offset, length) -> {}, 8);
                Thread.onSpinWait();
            }
        });
        drainer.start();

        journal.emit(out, 7_000L, 42L, new BackoffIdleStrategy());
        drainer.join(10_000L);

        assertEquals(3L, ring.producerPosition(), "every event must reach the ring");
        assertEquals(3L, ring.consumerPosition(), "the drainer consumed all events");
        assertTrue(metrics.journalBackpressureEvents() >= 1L, "the stall must be counted");
    }
}
