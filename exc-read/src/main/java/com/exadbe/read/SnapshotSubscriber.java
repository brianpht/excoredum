package com.exadbe.read;

import com.exadbe.engine.MatchingEngine;
import com.exadbe.journal.JournalStreams;
import com.exadbe.protocol.MessageHeaderDecoder;
import com.exadbe.protocol.SnapshotHeaderDecoder;
import com.exadbe.snapshot.SnapshotManager;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.logbuffer.FragmentHandler;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.collections.LongHashSet;

/**
 * Loads the newest service snapshot from the active source's Archive into the
 * replica's engine, fast-forwarding it to the snapshot's cluster-global
 * logPosition so the live-log replay only has to catch up the tail. The service
 * snapshot recording is prefixed with cluster-schema framing (schema 111), which
 * the loader skips to reach the excoredum {@code SnapshotHeader}; records are
 * fed through the engine's existing {@link SnapshotManager} load path.
 *
 * <p>An advance-only guard applies: a snapshot whose logPosition does not advance
 * the replica's applied position is skipped, so an older snapshot found on a
 * failover source can never roll state back. A snapshot failing the integrity
 * check is discarded (the engine is cleared) and reported as
 * {@link Result#CORRUPT}, so the caller can rebuild from the log start.
 *
 * <p>Poll-driven from the replica's single thread; {@link #start} initiates the
 * load and {@link #poll(int)} feeds replay fragments until {@link #isComplete()}.
 */
final class SnapshotSubscriber implements AutoCloseable {

    private static final int SNIFF_STREAM_ID = 47;
    // Bounded so a stuck endpoint resolution or stalled candidate sniff never
    // freezes the replica's single poll thread (and thus query serving) for more
    // than a couple of seconds; candidates are cached so each recording is
    // sniffed at most once per replica lifetime.
    private static final long RESOLVE_ENDPOINT_TIMEOUT_MS = 2_000L;
    private static final long SNIFF_TIMEOUT_MS = 2_000L;

    /** Outcome of a snapshot load attempt. */
    enum Result {
        /** A newer snapshot was loaded; see {@link #loadedLogPosition()}. */
        LOADED,
        /** No newer snapshot existed (or the load could not start). */
        SKIPPED,
        /** A snapshot failed the integrity check and was discarded. */
        CORRUPT,
        /** The load failed for a transient reason. */
        FAILED
    }

    private static final class Recording {
        final long id;
        final long startPos;
        final int streamId;

        Recording(final long id, final long startPos, final int streamId) {
            this.id = id;
            this.startPos = startPos;
            this.streamId = streamId;
        }
    }

    private static final class SnapshotRecording {
        final long recordingId;
        final long startPos;
        final long logPosition;

        SnapshotRecording(final long recordingId, final long startPos, final long logPosition) {
            this.recordingId = recordingId;
            this.startPos = startPos;
            this.logPosition = logPosition;
        }
    }

    private final MatchingEngine engine;
    private final String localHost;
    private final SnapshotManager snapshotManager = new SnapshotManager();
    private final MessageHeaderDecoder excHeader = new MessageHeaderDecoder();
    private final FragmentHandler fragmentHandler = this::onFragment;
    private final LongHashSet sniffedRecordings = new LongHashSet();

    private AeronArchive archive;
    private Subscription subscription;
    private boolean loadStarted;
    private boolean complete;
    private Result result = Result.FAILED;
    private long loadedLogPosition = -1L;

    SnapshotSubscriber(final MatchingEngine engine, final String localHost) {
        this.engine = engine;
        this.localHost = localHost;
    }

    /**
     * Finds the newest service snapshot whose logPosition advances
     * {@code minLogPosition} and starts loading it into the engine.
     *
     * @return {@code true} when a load is in progress (poll until
     *     {@link #isComplete()}); {@code false} when nothing newer exists or the
     *     load could not start.
     */
    boolean start(final AeronArchive archive, final long minLogPosition) {
        this.archive = archive;
        final SnapshotRecording candidate = findNewestCandidate(archive);
        if (candidate == null || candidate.logPosition <= minLogPosition) {
            return false;
        }
        final Subscription sub = archive.context()
                .aeron()
                .addSubscription("aeron:udp?endpoint=" + localHost + ":0", ReadStreams.SNAPSHOT_REPLAY);
        final String endpoint = awaitResolvedEndpoint(sub);
        if (endpoint == null) {
            sub.close();
            return false;
        }
        try {
            archive.startReplay(
                    candidate.recordingId,
                    candidate.startPos,
                    AeronArchive.NULL_LENGTH,
                    "aeron:udp?endpoint=" + endpoint,
                    ReadStreams.SNAPSHOT_REPLAY);
        } catch (final RuntimeException e) {
            sub.close();
            return false;
        }
        this.subscription = sub;
        return true;
    }

    /** Advances the snapshot load; call until {@link #isComplete()}. */
    int poll(final int limit) {
        if (subscription == null || complete) {
            return 0;
        }
        return subscription.poll(fragmentHandler, limit);
    }

    boolean isComplete() {
        return complete;
    }

    /** Whether the engine stores were cleared for a load (may leave partial state if aborted). */
    boolean isLoadStarted() {
        return loadStarted;
    }

    Result result() {
        return result;
    }

    /** The snapshot's cluster-global logPosition when {@link Result#LOADED}. */
    long loadedLogPosition() {
        return loadedLogPosition;
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
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return;
        }
        excHeader.wrap(buffer, offset);
        if (excHeader.schemaId() != MessageHeaderDecoder.SCHEMA_ID) {
            // Cluster-schema framing that prefixes the service snapshot.
            return;
        }
        // A record claiming a block longer than the fragment would let the
        // record decoder read adjacent bytes; drop it.
        if (MessageHeaderDecoder.ENCODED_LENGTH + excHeader.blockLength() > length) {
            return;
        }
        if (!loadStarted) {
            engine.beginSnapshotLoad(snapshotManager);
            loadStarted = true;
        }
        snapshotManager.onRecord(buffer, offset);
        if (snapshotManager.loadComplete()) {
            complete = true;
            loadedLogPosition = snapshotManager.loadedLogPosition();
            if (snapshotManager.verifyInvariant()) {
                result = Result.LOADED;
            } else {
                engine.clearState();
                result = Result.CORRUPT;
            }
            subscription.close();
            subscription = null;
        }
    }

    private SnapshotRecording findNewestCandidate(final AeronArchive archive) {
        final List<Recording> all = listAllRecordings(archive);
        all.sort(Comparator.comparingLong(r -> -r.id));
        for (final Recording recording : all) {
            if (sniffedRecordings.contains(recording.id)) {
                continue;
            }
            sniffedRecordings.add(recording.id);
            // Never sniff live non-snapshot recordings: replaying one streams
            // its traffic until the sniff deadline and blocks the poll thread.
            if (recording.streamId == ReadStreams.CONSENSUS_LOG_STREAM_ID
                    || recording.streamId == JournalStreams.JOURNAL_STREAM_ID) {
                continue;
            }
            final long logPosition = sniffLogPosition(archive, recording);
            if (logPosition > 0L) {
                return new SnapshotRecording(recording.id, recording.startPos, logPosition);
            }
        }
        return null;
    }

    /** Replays the head of one recording and decodes the service snapshot's logPosition, or -1. */
    private long sniffLogPosition(final AeronArchive archive, final Recording recording) {
        final int streamId = SNIFF_STREAM_ID;
        try (Subscription sub =
                archive.context().aeron().addSubscription("aeron:udp?endpoint=" + localHost + ":0", streamId)) {
            final String endpoint = awaitResolvedEndpoint(sub);
            archive.startReplay(
                    recording.id,
                    recording.startPos,
                    AeronArchive.NULL_LENGTH,
                    "aeron:udp?endpoint=" + endpoint,
                    streamId);
            final MessageHeaderDecoder header = new MessageHeaderDecoder();
            final SnapshotHeaderDecoder snapshotHeader = new SnapshotHeaderDecoder();
            final long[] found = {-1L};
            final long deadline = System.currentTimeMillis() + SNIFF_TIMEOUT_MS;
            boolean hadImage = false;
            long lastFragmentMs = System.currentTimeMillis();
            while (System.currentTimeMillis() < deadline && found[0] < 0L) {
                final int consumed = sub.poll(
                        (buffer, offset, fragmentLength, fragmentHeader) -> {
                            header.wrap(buffer, offset);
                            if (header.schemaId() == MessageHeaderDecoder.SCHEMA_ID
                                    && header.templateId() == SnapshotHeaderDecoder.TEMPLATE_ID) {
                                snapshotHeader.wrap(
                                        buffer,
                                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                                        header.blockLength(),
                                        header.version());
                                found[0] = snapshotHeader.logPosition();
                            }
                        },
                        64);
                if (consumed > 0) {
                    hadImage = true;
                    lastFragmentMs = System.currentTimeMillis();
                } else if (System.currentTimeMillis() - lastFragmentMs > 500L) {
                    // A snapshot header, if present, appears in the first
                    // fragments; an idle recording (stopped replay or a live
                    // non-snapshot stream) has nothing more to offer.
                    break;
                } else if (hadImage && sub.imageCount() == 0) {
                    break; // the stopped recording has been fully replayed
                }
                Thread.onSpinWait();
            }
            return found[0];
        } catch (final RuntimeException e) {
            return -1L;
        }
    }

    private static List<Recording> listAllRecordings(final AeronArchive archive) {
        final List<Recording> out = new ArrayList<>();
        final RecordingDescriptorConsumer consumer =
                (controlSessionId,
                        correlationId,
                        recordingId,
                        startTimestamp,
                        stopTimestamp,
                        startPosition,
                        stopPosition,
                        initialTermId,
                        segmentFileLength,
                        termBufferLength,
                        mtuLength,
                        sessionId,
                        streamId,
                        strippedChannel,
                        originalChannel,
                        sourceIdentity) -> out.add(new Recording(recordingId, startPosition, streamId));
        ArchiveRecordings.forEach(archive, consumer);
        return out;
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
}
