package io.justrade.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.justrade.config.CoreConfig;
import io.justrade.engine.MatchingEngine;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.protocol.MessageHeaderDecoder;
import io.justrade.protocol.QueryStreams;
import io.justrade.protocol.SnapshotFooterDecoder;
import io.justrade.read.config.ReadReplicaConfig;
import io.justrade.snapshot.SnapshotManager;
import io.justrade.telemetry.CoreMetrics;
import io.justrade.write.client.BackpressureException;
import io.justrade.write.client.ResultHandler;
import io.justrade.write.client.WriteClient;
import io.justrade.write.client.config.ClientConfig;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P11 snapshot negative paths: a fabricated snapshot recording is published to
 * the followed archive to exercise the loader's two defensive branches, which
 * no cluster test reaches on a healthy log.
 *
 * <ul>
 *   <li>A snapshot whose logPosition does not advance the replica is skipped by
 *       the advance-only guard: no load, no integrity failure, state untouched.
 *   <li>A snapshot with a newer logPosition but a broken checksum is discarded
 *       as CORRUPT; the replica clears the engine and rebuilds from the log
 *       start, converging to the true cluster state.
 * </ul>
 */
@Tag("cluster")
class SnapshotNegativeClusterTest {

    private static final long TIMEOUT_MS = 120_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final int FAKE_STREAM_ID = 99;
    private static final AtomicInteger PORT_COUNTER = new AtomicInteger(23299);
    private static final long WRONG_CHECKSUM = 0xDEAD_BEEF_CAFE_00L;

    @Test
    @Timeout(300)
    void staleSnapshotIsSkippedByTheAdvanceOnlyGuard(@TempDir final Path baseDir) throws Exception {
        final ClusterConfig config = ClusterConfig.singleNodeLocalhost(0, baseDir);
        final ClusterNode node = new ClusterNode(config, CoreConfig.defaults());
        try (Harness harness = new Harness(baseDir)) {
            final long[] lastIdLo = {-1L};
            final ResultHandler handler =
                    (idHi, lo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> lastIdLo[0] = lo;
            final ClientConfig clientConfig = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1))
                    .keepaliveIntervalNs(0L)
                    .build();
            try (WriteClient client = new WriteClient(clientConfig, handler)) {
                final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder(
                                baseDir.resolve("replica").resolve("driver").toString())
                        .channels(config.archiveControlChannel())
                        .localHost("localhost")
                        .query(QueryStreams.QUERY_REQUEST_CHANNEL, QueryStreams.QUERY_REQUEST_STREAM_ID)
                        .snapshotPollIntervalMs(250L)
                        .build();
                try (ReadReplica replica = new ReadReplica(replicaConfig, CoreConfig.defaults())) {
                    submit(client, () -> client.addSymbol(SYM, BASE, QUOTE, 1L, 1L));
                    for (long uid = 1L; uid <= 5L; uid++) {
                        final long u = uid;
                        submit(client, () -> client.addUser(u));
                        submit(client, () -> client.adjustBalance(u, BASE, 1_000L));
                    }
                    drain(client, replica, () -> replica.userCount() == 5);
                    settle(client, replica);
                    final long position = replica.appliedPosition();
                    final long stateHash = replica.stateHash();
                    assertTrue(position > 0L, "the replica must consume the log first");

                    // Fabricate a snapshot whose logPosition is behind the
                    // replica: the advance-only guard must skip it untouched.
                    try (AeronArchive archive = harness.connect(config.archiveControlChannel())) {
                        fabricateSnapshot(archive, harness.aeron(), position - 100L, false);
                    }
                    // Poll well past several snapshot intervals so the guard
                    // provably ran before the assertions.
                    final long created = System.currentTimeMillis();
                    drain(client, replica, () -> System.currentTimeMillis() - created > 1_500L);

                    assertEquals(0L, replica.health().snapshotsLoaded(), "a stale snapshot must not load");
                    assertEquals(0L, replica.health().integrityFailures(), "a skipped snapshot is not corrupt");
                    assertEquals(position, replica.appliedPosition(), "the position must not move");
                    assertEquals(stateHash, replica.stateHash(), "the replicated state must not change");
                    assertEquals(5, replica.userCount(), "the replicated state must not change");

                    // The replica keeps following live after the skip.
                    submit(client, () -> client.addUser(6L));
                    drain(client, replica, () -> replica.userCount() == 6);
                }
            }
        } finally {
            node.close();
        }
    }

    @Test
    @Timeout(300)
    void corruptSnapshotIsDiscardedAndTheReplicaRebuilds(@TempDir final Path baseDir) throws Exception {
        final ClusterConfig config = ClusterConfig.singleNodeLocalhost(0, baseDir);
        final ClusterNode node = new ClusterNode(config, CoreConfig.defaults());
        try (Harness harness = new Harness(baseDir)) {
            final long[] lastIdLo = {-1L};
            final ResultHandler handler =
                    (idHi, lo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> lastIdLo[0] = lo;
            final ClientConfig clientConfig = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1))
                    .keepaliveIntervalNs(0L)
                    .build();
            try (WriteClient client = new WriteClient(clientConfig, handler)) {
                final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder(
                                baseDir.resolve("replica").resolve("driver").toString())
                        .channels(config.archiveControlChannel())
                        .localHost("localhost")
                        .query(QueryStreams.QUERY_REQUEST_CHANNEL, QueryStreams.QUERY_REQUEST_STREAM_ID)
                        .snapshotPollIntervalMs(250L)
                        .build();
                try (ReadReplica replica = new ReadReplica(replicaConfig, CoreConfig.defaults())) {
                    submit(client, () -> client.addSymbol(SYM, BASE, QUOTE, 1L, 1L));
                    for (long uid = 1L; uid <= 5L; uid++) {
                        final long u = uid;
                        submit(client, () -> client.addUser(u));
                        submit(client, () -> client.adjustBalance(u, BASE, 1_000L));
                    }
                    submit(client, () -> client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, 1L, 0));
                    drain(client, replica, () -> replica.userCount() == 5 && replica.orderCount() == 1);
                    settle(client, replica);
                    final long position = replica.appliedPosition();

                    // Fabricate a snapshot with a NEWER logPosition but a broken
                    // checksum: the guard passes it, the integrity check rejects
                    // it, and the replica rebuilds from the log start. Wait for
                    // the rejection before growing the log: once the applied
                    // position reaches the fabricated position the advance-only
                    // guard would skip the fake without ever checking it.
                    try (AeronArchive archive = harness.connect(config.archiveControlChannel())) {
                        fabricateSnapshot(archive, harness.aeron(), position + 100L, true);
                    }
                    drain(client, replica, () -> replica.health().integrityFailures() >= 1L);

                    // Batch 2 grows the log past the fabricated position, so the
                    // rebuild converges beyond it and later snapshot polls skip
                    // the (remembered-corrupt) fake.
                    for (long uid = 6L; uid <= 10L; uid++) {
                        final long u = uid;
                        submit(client, () -> client.addUser(u));
                        submit(client, () -> client.adjustBalance(u, BASE, 1_000L));
                    }
                    drain(
                            client,
                            replica,
                            () -> replica.userCount() == 10
                                    && replica.orderCount() == 1
                                    && replica.isHealthy()
                                    && replica.appliedPosition() > position + 100L);
                    assertEquals(
                            0L, replica.health().snapshotsLoaded(), "the corrupt snapshot must never count as loaded");
                    assertTrue(replica.userExists(1L), "the rebuild must restore the resting maker");
                }
            }
        } finally {
            node.close();
        }
    }

    /** The test's own media driver + archive client, independent of the replica's. */
    private static final class Harness implements AutoCloseable {
        private final MediaDriver driver;
        private final Aeron aeron;

        Harness(final Path baseDir) {
            final String dir = baseDir.resolve("harness-driver").toString();
            this.driver = MediaDriver.launch(new MediaDriver.Context()
                    .aeronDirectoryName(dir)
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true));
            this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(dir));
        }

        Aeron aeron() {
            return aeron;
        }

        AeronArchive connect(final String controlChannel) {
            return AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .controlRequestChannel(controlChannel)
                    .controlResponseChannel("aeron:udp?endpoint=localhost:0")
                    .messageTimeoutNs(TimeUnit.SECONDS.toNanos(30)));
        }

        @Override
        public void close() {
            aeron.close();
            driver.close();
        }
    }

    /** Publishes a fabricated snapshot recording on a non-consensus stream. */
    private static void fabricateSnapshot(
            final AeronArchive archive, final Aeron aeron, final long logPosition, final boolean corrupt) {
        final List<byte[]> records = snapshotRecords(logPosition);
        if (corrupt) {
            corruptChecksum(records);
        }
        // A fresh endpoint per fabrication: a lingering recorder from a previous
        // test on the same port would swallow the frames without capturing them.
        final String channel = fakeChannel();
        final ExclusivePublication publication = aeron.addExclusivePublication(channel, FAKE_STREAM_ID);
        // The harness publishes from its own media driver, so the followed
        // archive sees the frames as a remote publication.
        archive.startRecording(channel, FAKE_STREAM_ID, SourceLocation.REMOTE);
        try {
            // The recorder subscription must be live before the first frame is
            // offered, or the recording silently captures nothing.
            awaitRecordingStarted(archive);
            awaitPublicationConnected(publication);
            publishRecords(publication, records);
            // Wait until the archive holds every frame before stopping, so no
            // in-flight frame is lost to the publication close.
            awaitRecordingData(archive, totalLength(records));
        } finally {
            publication.close();
        }
        archive.stopRecording(channel, FAKE_STREAM_ID);
        awaitFakeRecording(archive);
    }

    /** A unique localhost endpoint for each fabricated recording. */
    private static String fakeChannel() {
        return "aeron:udp?endpoint=localhost:" + PORT_COUNTER.getAndIncrement();
    }

    /** Encodes a deterministic engine snapshot with the given resume position. */
    private static List<byte[]> snapshotRecords(final long logPosition) {
        final MatchingEngine engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        final List<byte[]> records = new ArrayList<>();
        engine.writeSnapshot(
                new SnapshotManager(),
                (buffer, offset, length) -> {
                    final byte[] record = new byte[length];
                    buffer.getBytes(offset, record, 0, length);
                    records.add(record);
                },
                () -> {},
                logPosition);
        return records;
    }

    /** Breaks the footer checksum so the loaded state fails the integrity check. */
    private static void corruptChecksum(final List<byte[]> records) {
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        for (final byte[] record : records) {
            header.wrap(new UnsafeBuffer(record), 0);
            if (header.templateId() == SnapshotFooterDecoder.TEMPLATE_ID) {
                new UnsafeBuffer(record).putLong(MessageHeaderDecoder.ENCODED_LENGTH, WRONG_CHECKSUM);
                return;
            }
        }
        throw new IllegalStateException("no footer record in the fabricated snapshot");
    }

    private static void publishRecords(final ExclusivePublication publication, final List<byte[]> records) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
        for (final byte[] record : records) {
            buffer.putBytes(0, record);
            final long deadline = System.currentTimeMillis() + 10_000L;
            long result = Publication.NOT_CONNECTED;
            while (System.currentTimeMillis() < deadline && result < 0L) {
                result = publication.offer(buffer, 0, record.length);
                if (result < 0L) {
                    Thread.onSpinWait();
                }
            }
            if (result < 0L) {
                throw new AssertionError("could not publish fabricated snapshot record: " + result);
            }
        }
    }

    private static void awaitPublicationConnected(final ExclusivePublication publication) {
        final long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline && !publication.isConnected()) {
            Thread.onSpinWait();
        }
        if (!publication.isConnected()) {
            throw new AssertionError("the publication never connected to the archive recorder");
        }
    }

    /** Sum of the record lengths, used to confirm the recording captured everything. */
    private static long totalLength(final List<byte[]> records) {
        long total = 0L;
        for (final byte[] record : records) {
            total += record.length;
        }
        return total;
    }

    private static void awaitRecordingData(final AeronArchive archive, final long expectedDataBytes) {
        final long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            final long[] found = {-1L, -1L};
            final RecordingDescriptorConsumer check =
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
                        if (streamId == FAKE_STREAM_ID) {
                            found[0] = recordingId;
                            found[1] = startPosition;
                        }
                    };
            archive.listRecordings(0L, 100, check);
            if (found[0] >= 0L) {
                final long position = archive.getRecordingPosition(found[0]);
                if (position >= found[1] + expectedDataBytes) {
                    return;
                }
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("the fabricated recording never captured its data");
    }

    private static void awaitRecordingStarted(final AeronArchive archive) {
        final long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            final boolean[] found = {false};
            final RecordingDescriptorConsumer check =
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
                        if (streamId == FAKE_STREAM_ID) {
                            found[0] = true;
                        }
                    };
            archive.listRecordings(0L, 100, check);
            if (found[0]) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("the fabricated snapshot recording session never started");
    }

    private static void awaitFakeRecording(final AeronArchive archive) {
        final long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            final long[] dataLength = {-1L};
            final RecordingDescriptorConsumer check =
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
                        if (streamId == FAKE_STREAM_ID) {
                            dataLength[0] = stopPosition - startPosition;
                        }
                    };
            archive.listRecordings(0L, 100, check);
            if (dataLength[0] > 0L) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("the fabricated snapshot recording never captured data");
    }

    /**
     * Waits until the applied position holds steady across consecutive polls, so
     * the tail of the committed log has been recorded and replayed: an earlier
     * signal like a user count can trigger while balance commands for that user
     * are still in flight.
     */
    private static void settle(final WriteClient client, final ReadReplica replica) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        long last = -1L;
        int stable = 0;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            replica.poll();
            final long position = replica.appliedPosition();
            if (position == last) {
                if (++stable >= 5) {
                    return;
                }
            } else {
                last = position;
                stable = 0;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("replica never settled at a stable position");
    }

    private static long submit(final WriteClient client, final LongSupplier command) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                return command.getAsLong();
            } catch (final BackpressureException e) {
                client.poll();
                Thread.onSpinWait();
            }
        }
        throw new AssertionError("could not submit within timeout");
    }

    private static void drain(final WriteClient client, final ReadReplica replica, final BooleanSupplier condition) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            replica.poll();
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("replica never reached the expected state: users=" + replica.userCount()
                + " orders=" + replica.orderCount()
                + " applied=" + replica.appliedPosition()
                + " healthy=" + replica.isHealthy()
                + " integrityFailures=" + replica.health().integrityFailures()
                + " snapshotsLoaded=" + replica.health().snapshotsLoaded()
                + " failovers=" + replica.health().failovers()
                + " source=" + replica.currentSource());
    }
}
