package io.justrade.read;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.justrade.journal.JournalDedup;
import io.justrade.protocol.JournalEventDecoder;
import io.justrade.protocol.MatcherEventType;
import io.justrade.protocol.MessageHeaderDecoder;
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
                long eventIndex,
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
    private long lastPosition;

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
        lastPosition = header.position();
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return;
        }
        headerDecoder.wrap(buffer, offset);
        if (headerDecoder.templateId() != JournalEventDecoder.TEMPLATE_ID) {
            return;
        }
        // A header that claims a block longer than the fragment must be dropped
        // before the decoder reads past the frame into adjacent bytes.
        if (MessageHeaderDecoder.ENCODED_LENGTH + headerDecoder.blockLength() > length) {
            return;
        }
        eventDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(),
                headerDecoder.version());
        final long logPosition = eventDecoder.logPosition();
        final long eventIndex = eventIndex(eventDecoder);
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

    // Prefers the uint32 extension field added in v5; falls back to the legacy
    // uint16 field for messages recorded before it (whose index never wrapped).
    private static long eventIndex(final JournalEventDecoder eventDecoder) {
        final long ext = eventDecoder.eventIndexExt();
        return ext == JournalEventDecoder.eventIndexExtNullValue() ? eventDecoder.eventIndex() : ext;
    }

    /** Count of unique events delivered to the listener. */
    public long unique() {
        return unique;
    }

    /** Count of re-delivered events dropped by dedup. */
    public long duplicates() {
        return duplicates;
    }

    /** The recording position of the most recently consumed fragment, or 0 before the first. */
    public long lastPosition() {
        return lastPosition;
    }
}
