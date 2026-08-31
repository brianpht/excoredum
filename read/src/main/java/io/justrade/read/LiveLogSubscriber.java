package io.justrade.read;

import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.logbuffer.FragmentHandler;
import io.justrade.core.CommandOutcome;
import io.justrade.engine.MatchingEngine;
import io.justrade.protocol.CommandEnvelopeDecoder;
import io.justrade.read.order.OrderLedger;
import org.agrona.DirectBuffer;

/**
 * Subscribes to the consensus module log recording on a cluster Archive and
 * applies service messages ({@link CommandEnvelopeDecoder}) to a
 * {@link MatchingEngine} in near real time, providing live log following.
 *
 * <p>Each consensus log fragment starts with a cluster-schema
 * {@link io.aeron.cluster.codecs.MessageHeader}; when its templateId is
 * {@link SessionMessageHeaderDecoder#TEMPLATE_ID} the fragment wraps a service
 * message. The subscriber reads the leader-assigned timestamp from the session
 * header, skips the consensus framing, and feeds the command to the engine.
 *
 * <p>Recording positions are cluster-global: every member records the same
 * committed consensus log, so {@code startPosition} is a valid replay boundary
 * on any member whose recording covers it. {@link #connect()} verifies the
 * recording covers the requested position and fails cleanly (rather than
 * throwing) when it does not or when the archive rejects the replay, so the
 * caller can fail over to another source.
 *
 * <p>Single-writer: this class owns no thread. The replica drives {@link #poll(int)}
 * from its caller's thread, the same thread that serves queries.
 */
final class LiveLogSubscriber implements AutoCloseable {

    private static final int CONSENSUS_FRAMING_LENGTH =
            io.aeron.cluster.codecs.MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.BLOCK_LENGTH;
    // Bounded so a stuck endpoint resolution cannot freeze the replica's poll
    // thread (and thus query serving) for the full default.
    private static final long RESOLVE_ENDPOINT_TIMEOUT_MS = 2_000L;

    private final AeronArchive archive;
    private final MatchingEngine engine;
    private final CommandOutcome outcome;
    private final OrderLedger ledger;
    private final ReplicaCommandListener listener;
    private final long startPosition;
    private final long stopPosition;
    private final String localHost;
    private final int replayStreamId;
    private final io.aeron.cluster.codecs.MessageHeaderDecoder consensusHeader =
            new io.aeron.cluster.codecs.MessageHeaderDecoder();
    private final SessionMessageHeaderDecoder sessionHeader = new SessionMessageHeaderDecoder();
    private final io.justrade.protocol.MessageHeaderDecoder excHeader = new io.justrade.protocol.MessageHeaderDecoder();
    private final CommandEnvelopeDecoder envelopeDecoder = new CommandEnvelopeDecoder();
    private final FragmentHandler fragmentHandler = this::onFragment;

    private Subscription subscription;
    private long lastPosition;
    private boolean hadImage;
    private boolean reachedStop;
    private long recordingId = -1L;
    private long recordingEndPosition = -1L;

    LiveLogSubscriber(
            final AeronArchive archive,
            final MatchingEngine engine,
            final CommandOutcome outcome,
            final OrderLedger ledger,
            final ReplicaCommandListener listener,
            final long startPosition,
            final String localHost) {
        this(archive, engine, outcome, ledger, listener, startPosition, localHost, ReadStreams.LIVE_LOG_REPLAY);
    }

    LiveLogSubscriber(
            final AeronArchive archive,
            final MatchingEngine engine,
            final CommandOutcome outcome,
            final OrderLedger ledger,
            final ReplicaCommandListener listener,
            final long startPosition,
            final String localHost,
            final int replayStreamId) {
        this(archive, engine, outcome, ledger, listener, startPosition, -1L, localHost, replayStreamId);
    }

    LiveLogSubscriber(
            final AeronArchive archive,
            final MatchingEngine engine,
            final CommandOutcome outcome,
            final OrderLedger ledger,
            final ReplicaCommandListener listener,
            final long startPosition,
            final long stopPosition,
            final String localHost,
            final int replayStreamId) {
        this.archive = archive;
        this.engine = engine;
        this.outcome = outcome;
        this.ledger = ledger;
        this.listener = listener == null ? ReplicaCommandListener.NONE : listener;
        this.startPosition = startPosition;
        this.stopPosition = stopPosition;
        this.localHost = localHost;
        this.replayStreamId = replayStreamId;
    }

    /**
     * Locates the consensus recording, verifies it covers {@code startPosition},
     * and starts a bounded replay plus the subscription that {@link #poll(int)}
     * drains. Must be called on the polling thread. Endpoint resolution is
     * bounded (not open-ended) so a stuck channel cannot freeze the poll thread.
     *
     * @return {@code true} when the replay started; {@code false} when no
     *     recording exists yet, the recording does not cover the requested
     *     position, or the archive rejected the replay - the caller retries or
     *     fails over.
     */
    boolean connect() {
        final long foundRecordingId;
        final long recordingStartPos;
        final long recordingStopPos;
        try {
            final long[] found = {-1L, -1L, -1L};
            findConsensusRecording(found);
            if (found[0] < 0L) {
                return false;
            }
            foundRecordingId = found[0];
            recordingStartPos = found[1];
            recordingStopPos = found[2];
        } catch (final RuntimeException e) {
            return false;
        }
        if (!covers(recordingStartPos, recordingStopPos, startPosition)) {
            return false;
        }
        this.recordingId = foundRecordingId;
        this.recordingEndPosition = queryRecordingEndPosition(foundRecordingId);
        final Subscription sub =
                archive.context().aeron().addSubscription("aeron:udp?endpoint=" + localHost + ":0", replayStreamId);
        final String endpoint = awaitResolvedEndpoint(sub);
        if (endpoint == null) {
            sub.close();
            return false;
        }
        try {
            archive.startReplay(
                    foundRecordingId,
                    startPosition,
                    AeronArchive.NULL_LENGTH,
                    "aeron:udp?endpoint=" + endpoint,
                    replayStreamId);
        } catch (final RuntimeException e) {
            sub.close();
            return false;
        }
        this.subscription = sub;
        this.lastPosition = startPosition;
        return true;
    }

    /**
     * Whether the recording {@code [startPos, stopPos]} (active when
     * {@code stopPos == NULL_POSITION}) covers {@code position}, which is what a
     * replay from {@code position} requires.
     */
    private static boolean covers(final long startPos, final long stopPos, final long position) {
        if (position < startPos) {
            return false;
        }
        return stopPos == AeronArchive.NULL_POSITION || position <= stopPos;
    }

    int poll(final int fragmentLimit) {
        if (subscription == null || reachedStop) {
            return 0;
        }
        if (!isCaughtUp()) {
            // Refreshes the catch-up goal while the initial replay is running;
            // the recording grows as new commands are committed.
            this.recordingEndPosition = queryRecordingEndPosition(recordingId);
        }
        if (subscription.imageCount() > 0) {
            hadImage = true;
        }
        return subscription.poll(fragmentHandler, fragmentLimit);
    }

    /**
     * Whether the replay has passed the recording position observed at (and
     * refreshed since) connect, i.e. following live rather than replaying
     * history. Also true once a bounded replay of a stopped recording finished
     * (its image closed); fail-closed while the position cannot be read.
     */
    boolean isCaughtUp() {
        return isReplayEnded() || (recordingEndPosition >= 0L && lastPosition >= recordingEndPosition);
    }

    /**
     * Whether the subscriber stopped applying at {@code stopPosition}: the next
     * fragment ends past the boundary, so everything at or before it was applied
     * and nothing after it was touched. Always false without a stop position.
     */
    boolean reachedStop() {
        return reachedStop;
    }

    private long queryRecordingEndPosition(final long id) {
        try {
            final long position = archive.getRecordingPosition(id);
            return position >= 0L ? position : recordingEndPosition;
        } catch (final RuntimeException e) {
            return recordingEndPosition;
        }
    }

    /** Whether the bounded replay caught up to an idle recording and its image closed. */
    boolean isReplayEnded() {
        return subscription != null && hadImage && subscription.imageCount() == 0;
    }

    long lastPosition() {
        return lastPosition;
    }

    /** The recording id currently followed, or {@code -1} when not connected. */
    long recordingId() {
        return recordingId;
    }

    @Override
    public void close() {
        if (subscription != null) {
            subscription.close();
            subscription = null;
        }
    }

    private void onFragment(
            final DirectBuffer buffer, final int offset, final int length, final io.aeron.logbuffer.Header header) {
        if (stopPosition >= 0L && header.position() > stopPosition) {
            // Past the requested boundary: leave it for the live replay so the
            // engine and ledger stay aligned on exactly the same prefix.
            reachedStop = true;
            return;
        }
        lastPosition = header.position();
        if (length < CONSENSUS_FRAMING_LENGTH) {
            return;
        }
        consensusHeader.wrap(buffer, offset);
        if (consensusHeader.schemaId() != io.aeron.cluster.codecs.MessageHeaderDecoder.SCHEMA_ID
                || consensusHeader.templateId() != SessionMessageHeaderDecoder.TEMPLATE_ID) {
            return;
        }
        sessionHeader.wrap(
                buffer,
                offset + io.aeron.cluster.codecs.MessageHeaderDecoder.ENCODED_LENGTH,
                consensusHeader.blockLength(),
                consensusHeader.version());
        final long timestamp = sessionHeader.timestamp();

        final int serviceOffset = offset + CONSENSUS_FRAMING_LENGTH;
        if (serviceOffset + io.justrade.protocol.MessageHeaderDecoder.ENCODED_LENGTH > offset + length) {
            return;
        }
        excHeader.wrap(buffer, serviceOffset);
        if (excHeader.templateId() != CommandEnvelopeDecoder.TEMPLATE_ID) {
            return;
        }
        // A header that claims a block longer than the fragment must be dropped
        // before the decoder reads past the frame into adjacent bytes.
        if (serviceOffset + io.justrade.protocol.MessageHeaderDecoder.ENCODED_LENGTH + excHeader.blockLength()
                > offset + length) {
            return;
        }
        envelopeDecoder.wrap(
                buffer,
                serviceOffset + io.justrade.protocol.MessageHeaderDecoder.ENCODED_LENGTH,
                excHeader.blockLength(),
                excHeader.version());
        engine.process(envelopeDecoder, timestamp, outcome);
        ledger.applyCommand(timestamp, envelopeDecoder, outcome);
        listener.onCommand(timestamp, envelopeDecoder, outcome);
    }

    private static String awaitResolvedEndpoint(final Subscription subscription) {
        final long deadline = System.currentTimeMillis() + RESOLVE_ENDPOINT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            final String endpoint = subscription.resolvedEndpoint();
            if (endpoint != null) {
                return endpoint;
            }
            Thread.onSpinWait();
        }
        return null;
    }

    /** Fills {@code out} with the latest consensus recording's {@code [id, startPos, stopPos]}. */
    private void findConsensusRecording(final long[] out) {
        final RecordingDescriptorConsumer consumer =
                (controlSessionId,
                        correlationId,
                        recordingId,
                        startTimestamp,
                        stopTimestamp,
                        startPos,
                        stopPos,
                        initialTermId,
                        segmentFileLength,
                        termBufferLength,
                        mtuLength,
                        sessionId,
                        streamId,
                        strippedChannel,
                        originalChannel,
                        sourceIdentity) -> {
                    if (streamId == ReadStreams.CONSENSUS_LOG_STREAM_ID && recordingId > out[0]) {
                        out[0] = recordingId;
                        out[1] = startPos;
                        out[2] = stopPos;
                    }
                };
        ArchiveRecordings.forEach(archive, consumer);
    }
}
