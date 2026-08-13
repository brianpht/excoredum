package com.exadbe.read;

import com.exadbe.core.CommandOutcome;
import com.exadbe.engine.MatchingEngine;
import com.exadbe.protocol.CommandEnvelopeDecoder;
import com.exadbe.read.order.OrderLedger;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.logbuffer.FragmentHandler;
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
 * <p>Single-writer: this class owns no thread. The replica drives {@link #poll(int)}
 * from its caller's thread, the same thread that serves queries.
 */
final class LiveLogSubscriber implements AutoCloseable {

    private static final int CONSENSUS_FRAMING_LENGTH =
            io.aeron.cluster.codecs.MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.BLOCK_LENGTH;
    private static final long RESOLVE_ENDPOINT_TIMEOUT_MS = 10_000L;

    private final AeronArchive archive;
    private final MatchingEngine engine;
    private final CommandOutcome outcome;
    private final OrderLedger ledger;
    private final ReplicaCommandListener listener;
    private final long startPosition;
    private final String localHost;
    private final io.aeron.cluster.codecs.MessageHeaderDecoder consensusHeader =
            new io.aeron.cluster.codecs.MessageHeaderDecoder();
    private final SessionMessageHeaderDecoder sessionHeader = new SessionMessageHeaderDecoder();
    private final com.exadbe.protocol.MessageHeaderDecoder excHeader = new com.exadbe.protocol.MessageHeaderDecoder();
    private final CommandEnvelopeDecoder envelopeDecoder = new CommandEnvelopeDecoder();
    private final FragmentHandler fragmentHandler = this::onFragment;

    private Subscription subscription;
    private long lastPosition;
    private boolean hadImage;
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
        this.archive = archive;
        this.engine = engine;
        this.outcome = outcome;
        this.ledger = ledger;
        this.listener = listener == null ? ReplicaCommandListener.NONE : listener;
        this.startPosition = startPosition;
        this.localHost = localHost;
    }

    boolean connect() {
        final long foundRecordingId = findConsensusRecording();
        if (foundRecordingId < 0) {
            return false;
        }
        this.recordingId = foundRecordingId;
        this.recordingEndPosition = queryRecordingEndPosition(foundRecordingId);
        final Subscription sub = archive.context()
                .aeron()
                .addSubscription("aeron:udp?endpoint=" + localHost + ":0", ReadStreams.LIVE_LOG_REPLAY);
        final String endpoint = awaitResolvedEndpoint(sub);
        if (endpoint == null) {
            sub.close();
            return false;
        }
        final String replayChannel = "aeron:udp?endpoint=" + endpoint;
        archive.startReplay(
                foundRecordingId, startPosition, AeronArchive.NULL_LENGTH, replayChannel, ReadStreams.LIVE_LOG_REPLAY);
        this.subscription = sub;
        this.lastPosition = startPosition;
        return true;
    }

    int poll(final int fragmentLimit) {
        if (subscription == null) {
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

    private long queryRecordingEndPosition(final long id) {
        final long position = archive.getRecordingPosition(id);
        return position >= 0L ? position : recordingEndPosition;
    }

    /** Whether the bounded replay caught up to an idle recording and its image closed. */
    boolean isReplayEnded() {
        return subscription != null && hadImage && subscription.imageCount() == 0;
    }

    long lastPosition() {
        return lastPosition;
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
        if (serviceOffset + com.exadbe.protocol.MessageHeaderDecoder.ENCODED_LENGTH > offset + length) {
            return;
        }
        excHeader.wrap(buffer, serviceOffset);
        if (excHeader.templateId() != CommandEnvelopeDecoder.TEMPLATE_ID) {
            return;
        }
        envelopeDecoder.wrap(
                buffer,
                serviceOffset + com.exadbe.protocol.MessageHeaderDecoder.ENCODED_LENGTH,
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

    private long findConsensusRecording() {
        final long[] latest = {-1L};
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
                    if (streamId == ReadStreams.CONSENSUS_LOG_STREAM_ID && recordingId > latest[0]) {
                        latest[0] = recordingId;
                    }
                };
        archive.listRecordings(0L, 200, consumer);
        return latest[0];
    }
}
