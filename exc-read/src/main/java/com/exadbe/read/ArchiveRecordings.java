package com.exadbe.read;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;

/**
 * Paginated recording enumeration. The {@code listRecordings(0, N, ...)} cap in
 * callers misses the newest recording once an archive accumulates more than N
 * recordings (every clean start adds recordings), silently wiping replica state
 * or letting HA consumers pick a stale recording. This helper pages through the
 * archive with a fixed page size until every descriptor is delivered.
 *
 * <p>Cold path only; the closure-based iteration runs on the single polling
 * thread during connect, not per event.
 */
final class ArchiveRecordings {

    private static final int PAGE_SIZE = 512;

    private ArchiveRecordings() {}

    /** Invokes {@code consumer} for every recording, oldest to newest, with no cap. */
    static void forEach(final AeronArchive archive, final RecordingDescriptorConsumer consumer) {
        long fromId = 0L;
        while (true) {
            final long[] count = {0L};
            final long[] maxId = {-1L};
            archive.listRecordings(
                    fromId,
                    PAGE_SIZE,
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
                            sourceIdentity) -> {
                        count[0]++;
                        if (recordingId > maxId[0]) {
                            maxId[0] = recordingId;
                        }
                        consumer.onRecordingDescriptor(
                                controlSessionId,
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
                                sourceIdentity);
                    });
            if (count[0] < PAGE_SIZE) {
                return;
            }
            if (maxId[0] <= fromId - 1) {
                return; // no forward progress; defensive against a misbehaving archive
            }
            fromId = maxId[0] + 1L;
        }
    }

    /** The id of the newest recording for {@code streamId}, or {@code -1} when none exists. */
    static long latestRecordingId(final AeronArchive archive, final int streamId) {
        final long[] latest = {-1L};
        forEach(
                archive,
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
                        sId,
                        strippedChannel,
                        originalChannel,
                        sourceIdentity) -> {
                    if (sId == streamId && recordingId > latest[0]) {
                        latest[0] = recordingId;
                    }
                });
        return latest[0];
    }
}
