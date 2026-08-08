package com.exadbe.launcher;

import com.exadbe.journal.EventJournalRing;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.IdleStrategy;

/**
 * Drains the domain-event ring on a dedicated thread and offers each event to an
 * Aeron publication (recorded by the Archive on the journal stream). Runs off the
 * consensus thread so journal I/O never stalls Raft. On back-pressure it retries
 * with an idle strategy and never drops an event.
 */
public final class EventJournalRecorder implements Agent, EventJournalRing.EventHandler {

    private final EventJournalRing ring;
    private final ExclusivePublication publication;
    private final IdleStrategy offerIdle;
    private final int fragmentLimit;

    private volatile long published;

    public EventJournalRecorder(
            final EventJournalRing ring,
            final ExclusivePublication publication,
            final IdleStrategy offerIdle,
            final int fragmentLimit) {
        this.ring = ring;
        this.publication = publication;
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
                throw new IllegalStateException("Journal publication unavailable: " + result);
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
}
