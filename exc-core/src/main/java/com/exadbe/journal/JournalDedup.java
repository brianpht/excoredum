package com.exadbe.journal;

/**
 * Consumer-side idempotency gate for the domain-event journal. The stream is
 * strictly increasing in {@code (logPosition, eventIndex)}, so an event
 * re-published after a failover carries a key not greater than the last accepted
 * one and is rejected. Single-consumer; allocation-free.
 */
public final class JournalDedup {

    private long lastLogPosition = -1L;
    private int lastEventIndex = -1;

    /** Returns {@code true} the first time a key is seen; {@code false} for a duplicate. */
    public boolean accept(final long logPosition, final int eventIndex) {
        if (logPosition > lastLogPosition || (logPosition == lastLogPosition && eventIndex > lastEventIndex)) {
            lastLogPosition = logPosition;
            lastEventIndex = eventIndex;
            return true;
        }
        return false;
    }
}
