package com.exadbe.read;

import com.exadbe.journal.JournalDedup;
import com.exadbe.protocol.JournalEventDecoder;
import com.exadbe.protocol.MatcherEventType;
import com.exadbe.protocol.MessageHeaderDecoder;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Reads domain events from a journal fragment stream (a live subscription or an
 * Archive replay) and delivers each committed event exactly once. A
 * {@link JournalDedup} drops events re-delivered across a failover, so consumers
 * (audit, analytics, risk) can follow the stream idempotently.
 *
 * <p>Single-consumer, poll-driven from one thread.
 */
public final class JournalConsumer {

    /** Receives one unique domain event. */
    @FunctionalInterface
    public interface Listener {
        void onEvent(
                long logPosition,
                int eventIndex,
                MatcherEventType type,
                int symbolId,
                long makerOrderId,
                long makerUid,
                long takerUid,
                long price,
                long size,
                boolean makerCompleted);
    }

    private final Subscription subscription;
    private final Listener listener;
    private final JournalDedup dedup;
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final JournalEventDecoder eventDecoder = new JournalEventDecoder();
    private final FragmentHandler fragmentHandler = this::onFragment;

    private long unique;
    private long duplicates;

    public JournalConsumer(final Subscription subscription, final Listener listener) {
        this(subscription, listener, new JournalDedup());
    }

    /** Uses a shared dedup so several sources (e.g. failover archives) merge idempotently. */
    public JournalConsumer(final Subscription subscription, final Listener listener, final JournalDedup dedup) {
        this.subscription = subscription;
        this.listener = listener;
        this.dedup = dedup;
    }

    /** Polls the stream, delivering up to {@code limit} deduped events; call in a loop. */
    public int poll(final int limit) {
        return subscription.poll(fragmentHandler, limit);
    }

    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        headerDecoder.wrap(buffer, offset);
        if (headerDecoder.templateId() != JournalEventDecoder.TEMPLATE_ID) {
            return;
        }
        eventDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(),
                headerDecoder.version());
        final long logPosition = eventDecoder.logPosition();
        final int eventIndex = eventDecoder.eventIndex();
        if (!dedup.accept(logPosition, eventIndex)) {
            duplicates++;
            return;
        }
        unique++;
        listener.onEvent(
                logPosition,
                eventIndex,
                eventDecoder.eventType(),
                eventDecoder.symbolId(),
                eventDecoder.makerOrderId(),
                eventDecoder.makerUid(),
                eventDecoder.takerUid(),
                eventDecoder.price(),
                eventDecoder.size(),
                eventDecoder.makerCompleted() != 0);
    }

    /** Count of unique events delivered to the listener. */
    public long unique() {
        return unique;
    }

    /** Count of re-delivered events dropped by dedup. */
    public long duplicates() {
        return duplicates;
    }
}
