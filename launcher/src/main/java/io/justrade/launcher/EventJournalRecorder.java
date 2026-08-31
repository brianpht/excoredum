package io.justrade.launcher;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.justrade.journal.EventJournalRing;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.IdleStrategy;

/**
 * Drains the domain-event ring on a dedicated thread and offers each event to an
 * Aeron publication (recorded by the Archive on the journal stream). Runs off the
 * consensus thread so journal I/O never stalls Raft. On back-pressure it retries
 * with an idle strategy and never drops an event.
 *
 * <p>A publication that becomes unusable ({@code CLOSED} /
 * {@code MAX_POSITION_EXCEEDED}) is re-established through the supplied recovery
 * callback instead of failing the agent, so the ring keeps draining after a
 * transient archive or driver fault. Each failure increments an off-heap error
 * counter; no formatting or allocation happens on the path.
 */
public final class EventJournalRecorder implements Agent, EventJournalRing.EventHandler {

    /** Re-establishes the journal publication after it becomes unusable. */
    @FunctionalInterface
    public interface PublicationRecovery {
        ExclusivePublication recover();
    }

    /** Receives one notification per recovered publication failure; never formats. */
    @FunctionalInterface
    public interface ErrorSink {
        void record();
    }

    private final EventJournalRing ring;
    private final PublicationRecovery recovery;
    private final ErrorSink errors;
    private final IdleStrategy offerIdle;
    private final int fragmentLimit;

    private ExclusivePublication publication;
    private volatile long published;
    private volatile long recovered;

    public EventJournalRecorder(
            final EventJournalRing ring,
            final ExclusivePublication publication,
            final PublicationRecovery recovery,
            final ErrorSink errors,
            final IdleStrategy offerIdle,
            final int fragmentLimit) {
        this.ring = ring;
        this.publication = publication;
        this.recovery = recovery;
        this.errors = errors;
        this.offerIdle = offerIdle;
        this.fragmentLimit = fragmentLimit;
    }

    @Override
    public int doWork() {
        return ring.poll(this, fragmentLimit);
    }

    @Override
    public void onEvent(final DirectBuffer buffer, final int offset, final int length) {
        offerIdle.reset();
        while (true) {
            final long result = publication.offer(buffer, offset, length);
            if (result > 0L) {
                published++;
                return;
            }
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                errors.record();
                CloseHelper.quietClose(publication);
                publication = recovery.recover();
                recovered++;
                offerIdle.reset();
                continue;
            }
            offerIdle.idle();
        }
    }

    @Override
    public String roleName() {
        return "event-journal-recorder";
    }

    /** Total events published to the journal stream so far. */
    public long published() {
        return published;
    }

    /** Total times the journal publication had to be re-established. */
    public long recoveries() {
        return recovered;
    }
}
