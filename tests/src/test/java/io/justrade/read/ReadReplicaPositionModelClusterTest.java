package io.justrade.read;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.cluster.ClusterTool;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.justrade.config.CoreConfig;
import io.justrade.core.CommandOutcome;
import io.justrade.engine.MatchingEngine;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.protocol.MessageHeaderDecoder;
import io.justrade.protocol.QueryStreams;
import io.justrade.protocol.SnapshotHeaderDecoder;
import io.justrade.read.config.ReadReplicaConfig;
import io.justrade.read.order.OrderLedger;
import io.justrade.read.report.ReportGenerator;
import io.justrade.telemetry.CoreMetrics;
import io.justrade.write.client.BackpressureException;
import io.justrade.write.client.ResultHandler;
import io.justrade.write.client.WriteClient;
import io.justrade.write.client.config.ClientConfig;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
 * Phase 0 of the read-replica failover improvement: empirically verifies the
 * archive position model that the resume-instead-of-rebuild design (ADR 0008 in
 * the reference project) relies on, before any code change adopts it.
 *
 * <p>Facts under test:
 * <ul>
 *   <li><b>A</b> - every member's Archive records the committed consensus log
 *       (stream 100) starting at position 0, and two members' recordings are
 *       byte-identical over the committed prefix, so recording positions are
 *       comparable across members.
 *   <li><b>B</b> - a service snapshot recording exists on every member's Archive
 *       and carries the same cluster-global {@code logPosition} (the stream id
 *       is discovered, not assumed).
 *   <li><b>C</b> - a fresh engine resuming a member's recording at the position
 *       another member had reached converges to exactly the state a full replay
 *       of that recording produces, and to the state the current rebuild-based
 *       replica converges to.
 *   <li><b>Restart</b> - a restarted member (cleanStart=false) opens a new
 *       consensus-log recording whose start position is the raft position where
 *       it stopped, so positions stay comparable across a member restart.
 * </ul>
 */
@Tag("cluster")
class ReadReplicaPositionModelClusterTest {

    private static final int NODES = 3;
    private static final int CONSENSUS_LOG_STREAM_ID = 100;
    private static final long TIMEOUT_MS = 60_000L;
    private static final long CAPTURE_TIMEOUT_MS = 15_000L;
    private static final long QUIESCENCE_MS = 2_000L;
    private static final long REPLAY_TIMEOUT_MS = 30_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;

    /** One recording descriptor. */
    private static final class Recording {
        final long id;
        final long startPos;
        final long stopPos;
        final int streamId;

        Recording(final long id, final long startPos, final long stopPos, final int streamId) {
            this.id = id;
            this.startPos = startPos;
            this.stopPos = stopPos;
            this.streamId = streamId;
        }

        @Override
        public String toString() {
            return "Recording{id=" + id + ", startPos=" + startPos + ", stopPos=" + stopPos + ", streamId=" + streamId
                    + "}";
        }
    }

    /** One replayed fragment: its recording position, length, and payload bytes. */
    private static final class Fragment {
        final long position;
        final int length;
        final byte[] payload;

        Fragment(final long position, final int length, final byte[] payload) {
            this.position = position;
            this.length = length;
            this.payload = payload;
        }
    }

    /** A service snapshot recording found by content sniffing. */
    private static final class SnapshotRecording {
        final long recordingId;
        final int streamId;
        final long logPosition;

        SnapshotRecording(final long recordingId, final int streamId, final long logPosition) {
            this.recordingId = recordingId;
            this.streamId = streamId;
            this.logPosition = logPosition;
        }
    }

    /** The test's own media driver + archive clients, independent of the replica's. */
    private static final class Harness implements AutoCloseable {
        private final MediaDriver driver;
        private final Aeron aeron;
        private final AtomicInteger replayStreamIds = new AtomicInteger(60);

        Harness(final Path baseDir) {
            final String dir = baseDir.resolve("harness-driver").toString();
            this.driver = MediaDriver.launch(new MediaDriver.Context()
                    .aeronDirectoryName(dir)
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true));
            this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(dir));
        }

        AeronArchive connect(final String controlChannel) {
            return AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .controlRequestChannel(controlChannel)
                    .controlResponseChannel("aeron:udp?endpoint=localhost:0")
                    .messageTimeoutNs(TimeUnit.SECONDS.toNanos(30)));
        }

        int nextReplayStreamId() {
            return replayStreamIds.getAndIncrement();
        }

        @Override
        public void close() {
            aeron.close();
            driver.close();
        }
    }

    // ------------------------------------------------------------------ Fact A

    @Test
    @Timeout(300)
    void everyMemberRecordsAnIdenticalCommittedPrefix(@TempDir final Path baseDir) throws Exception {
        final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODES, baseDir);
        final ClusterNode[] nodes = startNodes(configs);
        try (Harness harness = new Harness(baseDir)) {
            final Set<Long> results = new HashSet<>();
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) ->
                            results.add(idLo);
            final ClientConfig clientConfig = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                    .build();
            try (WriteClient client = new WriteClient(clientConfig, handler)) {
                submitBatch(results, client, configs);
            }

            try (AeronArchive archive0 = harness.connect(configs[0].archiveControlChannel());
                    AeronArchive archive1 = harness.connect(configs[1].archiveControlChannel())) {
                final Recording rec0 = latestConsensusRecording(archive0);
                final Recording rec1 = latestConsensusRecording(archive1);

                // Fact A-1: on a fresh cluster the consensus recording starts at 0.
                assertEquals(0L, rec0.startPos, "node-0's consensus recording must start at position 0");
                assertEquals(0L, rec1.startPos, "node-1's consensus recording must start at position 0");

                final List<Fragment> frag0 = captureFrom(harness, archive0, rec0.id, 0L);
                final List<Fragment> frag1 = captureFrom(harness, archive1, rec1.id, 0L);
                assertFalse(frag0.isEmpty(), "node-0's recording must replay at least one fragment");
                assertFalse(frag1.isEmpty(), "node-1's recording must replay at least one fragment");
                // The first frame is not at 0: the recording has a small reserved
                // prefix region, identical on every member. Cross-member position
                // comparability is what matters.
                assertEquals(
                        frag0.get(0).position,
                        frag1.get(0).position,
                        "both members' recordings must begin at the same position; node-0="
                                + positions(frag0, 4)
                                + " node-1="
                                + positions(frag1, 4));
                assertStrictlyIncreasing(frag0);
                assertStrictlyIncreasing(frag1);

                // Fact A-2: the committed prefixes are byte-identical, position for
                // position; the recordings may only differ by a trailing tail.
                final int common = Math.min(frag0.size(), frag1.size());
                for (int i = 0; i < common; i++) {
                    final Fragment a = frag0.get(i);
                    final Fragment b = frag1.get(i);
                    assertEquals(
                            a.position,
                            b.position,
                            "recording positions diverge at index "
                                    + i
                                    + " (node-0 size="
                                    + frag0.size()
                                    + ", node-1 size="
                                    + frag1.size()
                                    + ", node-0 tail="
                                    + positions(tail(frag0, 4), 4)
                                    + ", node-1 tail="
                                    + positions(tail(frag1, 4), 4)
                                    + ")");
                    assertEquals(a.length, b.length, "fragment lengths diverge at index " + i);
                    assertArrayEquals(a.payload, b.payload, "committed prefix payloads diverge at index " + i);
                }
                System.out.println("Fact A: node-0 fragments=" + frag0.size() + " node-1 fragments=" + frag1.size()
                        + " first positions " + positions(frag0, 4));
            }
        } finally {
            closeNodes(nodes);
        }
    }

    // ------------------------------------------------------------------ Fact B

    @Test
    @Timeout(300)
    void serviceSnapshotRecordingsShareAClusterGlobalLogPosition(@TempDir final Path baseDir) throws Exception {
        final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODES, baseDir);
        final ClusterNode[] nodes = startNodes(configs);
        try {
            final Set<Long> results = new HashSet<>();
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) ->
                            results.add(idLo);
            final ClientConfig clientConfig = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                    .build();
            try (WriteClient client = new WriteClient(clientConfig, handler)) {
                awaitLeader(client);
                submitBatch(results, client, configs);

                // Route the snapshot request through Raft: every member snapshots
                // at the same committed log position.
                final int leader = client.leaderMemberId();
                assertTrue(
                        ClusterTool.snapshot(configs[leader].clusterDir(), System.out),
                        "the snapshot request must be accepted by the consensus module");
            }

            try (Harness harness = new Harness(baseDir)) {
                long expectedLogPosition = -1L;
                int discoveredStreamId = -1;
                for (int i = 0; i < NODES; i++) {
                    try (AeronArchive archive = harness.connect(configs[i].archiveControlChannel())) {
                        final SnapshotRecording snapshot = awaitSnapshotRecording(harness, archive);
                        assertNotNull(snapshot, "member " + i + " must expose a service snapshot recording");
                        assertTrue(
                                snapshot.logPosition > 0L,
                                "the snapshot logPosition must be beyond the empty log, was: " + snapshot.logPosition);
                        assertNotEquals(
                                CONSENSUS_LOG_STREAM_ID,
                                snapshot.streamId,
                                "the service snapshot stream differs from the consensus log stream");
                        if (expectedLogPosition == -1L) {
                            expectedLogPosition = snapshot.logPosition;
                            discoveredStreamId = snapshot.streamId;
                        } else {
                            assertEquals(
                                    expectedLogPosition,
                                    snapshot.logPosition,
                                    "all members snapshot at the same cluster-global logPosition");
                            assertEquals(
                                    discoveredStreamId,
                                    snapshot.streamId,
                                    "the snapshot stream id is consistent across members");
                        }
                    }
                }
                assertTrue(discoveredStreamId > 0, "a service snapshot stream id must be discovered");
            }
        } finally {
            closeNodes(nodes);
        }
    }

    // ------------------------------------------------------------------ Fact C

    @Test
    @Timeout(300)
    void resumingFromAppliedPositionOnAnotherMemberConverges(@TempDir final Path baseDir) throws Exception {
        final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODES, baseDir);
        final ClusterNode[] nodes = startNodes(configs);
        try (Harness harness = new Harness(baseDir)) {
            final long[] lastIdLo = {-1L};
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) ->
                            lastIdLo[0] = idLo;
            final ClientConfig clientConfig = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                    .build();
            final ReadReplicaConfig replicaConfig = ReadReplicaConfig.localhost(
                    baseDir.resolve("replica").resolve("driver").toString(),
                    archiveChannels(configs),
                    "localhost",
                    QueryStreams.QUERY_REQUEST_CHANNEL,
                    QueryStreams.QUERY_REQUEST_STREAM_ID);

            try (WriteClient client = new WriteClient(clientConfig, handler);
                    ReadReplica replica = new ReadReplica(replicaConfig, CoreConfig.defaults())) {
                submitBatch1(client, lastIdLo, replica);
                pollUntil(client, replica, () -> replica.userCount() == 5 && replica.orderCount() == 1);
                assertEquals(0, replica.currentSource(), "the replica initially follows the primary source");

                final long replicaPosition = replica.appliedPosition();
                assertTrue(replicaPosition > 0L, "the replica must have consumed the log before the kill");

                // Capture BOTH members' recordings while both are alive: the
                // committed prefix is identical everywhere, so its end is the
                // highest position that is safe to resume from on any member.
                final List<Fragment> prefix0;
                final List<Fragment> prefix1;
                try (AeronArchive archive0 = harness.connect(configs[0].archiveControlChannel())) {
                    prefix0 = captureFrom(harness, archive0, latestConsensusRecording(archive0).id, 0L);
                }
                try (AeronArchive archive1 = harness.connect(configs[1].archiveControlChannel())) {
                    prefix1 = captureFrom(harness, archive1, latestConsensusRecording(archive1).id, 0L);
                }
                final long committedEnd = commonPrefixEndPosition(prefix0, prefix1);
                assertTrue(committedEnd > 0L, "the members must share a non-trivial committed prefix");
                System.out.println("Fact C: replicaPosition=" + replicaPosition
                        + " committedEnd=" + committedEnd
                        + " prefix0.size=" + prefix0.size()
                        + " prefix1.size=" + prefix1.size()
                        + " prefix0 tail=" + positions(tail(prefix0, 4), 4)
                        + " prefix1 tail=" + positions(tail(prefix1, 4), 4));

                // Kill the replica's source; the surviving quorum keeps serving.
                nodes[0].close();
                nodes[0] = null;

                submitBatch2(client, lastIdLo, replica);
                // The current implementation converges by rebuilding from the new
                // source; its final state is the reference for the resume path.
                pollUntil(client, replica, () -> replica.userCount() == 10 && replica.orderCount() == 2);
                assertNotEquals(0, replica.currentSource(), "the replica must fail over to another member");
                final long referenceHash = replica.stateHash();

                try (AeronArchive archive1 = harness.connect(configs[1].archiveControlChannel())) {
                    final Recording rec1 = latestConsensusRecording(archive1);
                    assertTrue(
                            rec1.startPos <= committedEnd, "the new source's recording must cover the resume position");

                    // The core claim: a fresh engine replaying the new member's
                    // recording up to the committed boundary, then resuming from
                    // that boundary on the same member, converges to the
                    // full-replay state - which is also the state the rebuild-based
                    // replica reached. The boundary frame is re-applied at most
                    // once and absorbed by engine dedup.
                    final long resumedHash = resumeStateHash(archive1, committedEnd);
                    final long fullReplayHash = replayStateHash(archive1, 0L);
                    System.out.println("Fact C: committedEnd=" + committedEnd
                            + " resumedHash=" + resumedHash
                            + " fullReplayHash=" + fullReplayHash
                            + " referenceHash=" + referenceHash);
                    assertEquals(
                            fullReplayHash,
                            resumedHash,
                            "resuming at the committed boundary must converge to the full-replay state");
                    assertEquals(
                            referenceHash,
                            resumedHash,
                            "the resumed state must match the replica's rebuilt reference state");
                }
            }
        } finally {
            closeNodes(nodes);
        }
    }

    // ------------------------------------------------------------------ Restart

    @Test
    @Timeout(300)
    void aRestartedMemberContinuesItsRecordingAtTheResumedPosition(@TempDir final Path baseDir) throws Exception {
        final ClusterConfig config = ClusterConfig.singleNodeLocalhost(0, baseDir);
        ClusterNode node = new ClusterNode(config, CoreConfig.defaults());
        long oldRecordingId;
        long preRestartPosition;
        try (Harness harness = new Harness(baseDir)) {
            final long[] lastIdLo = {-1L};
            final ResultHandler phaseOneHandler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) ->
                            lastIdLo[0] = idLo;
            try (WriteClient client = new WriteClient(
                    ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1)).build(), phaseOneHandler)) {
                awaitResult(client, client.addSymbol(SYM, BASE, QUOTE, 1L, 1L), lastIdLo);
                awaitResult(client, client.addUser(MAKER), lastIdLo);
                awaitResult(client, client.adjustBalance(MAKER, BASE, 1_000L), lastIdLo);
                awaitResult(client, client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, MAKER, 0), lastIdLo);
            }
            try (AeronArchive archive = harness.connect(config.archiveControlChannel())) {
                final Recording rec = latestConsensusRecording(archive);
                oldRecordingId = rec.id;
                assertEquals(0L, rec.startPos, "the first consensus recording starts at position 0");
                preRestartPosition = archive.getRecordingPosition(rec.id);
            }
            assertTrue(preRestartPosition > 0L, "the pre-restart recording must have advanced past 0");

            node.close();
            node = null;

            // Warm restart preserves the archive and cluster dirs.
            node = new ClusterNode(config, CoreConfig.defaults(), false);
            final long[] phaseTwoIdLo = {-1L};
            final long[] lastFilled = {-1L};
            final int[] trades = {0};
            final ResultHandler phaseTwoHandler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                        phaseTwoIdLo[0] = idLo;
                        if (hasFilledSize) {
                            lastFilled[0] = filledSize;
                        }
                    };
            try (WriteClient client = new WriteClient(
                    ClientConfig.builder(2L, ClusterConfig.ingressEndpoints(1)).build(), phaseTwoHandler)) {
                client.tradeListener(
                        (idHi, idLo, index, symbolId, makerOrderId, makerUid, takerUid, price, size, makerCompleted) ->
                                trades[0]++);
                awaitResult(client, client.addUser(TAKER), phaseTwoIdLo);
                awaitResult(client, client.adjustBalance(TAKER, QUOTE, 1_000_000L), phaseTwoIdLo);

                // The bid crosses the snapshot-recovered resting ask at 100.
                final long fillCommand = client.placeGtc(SYM, 2L, false, 105L, 6L, 105L, TAKER, 0);
                awaitResult(client, fillCommand, phaseTwoIdLo);
                assertEquals(6L, lastFilled[0], "the restarted member must recover the resting maker and fill it");

                final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
                while (System.currentTimeMillis() < deadline && trades[0] == 0) {
                    client.poll();
                    Thread.onSpinWait();
                }
                assertEquals(1, trades[0], "the recovered maker must be matched after the restart");
            }

            try (AeronArchive archive = harness.connect(config.archiveControlChannel())) {
                final List<Recording> recordings = listStreamRecordings(archive, CONSENSUS_LOG_STREAM_ID);
                System.out.println("Restart: pre-restart id=" + oldRecordingId + " position=" + preRestartPosition
                        + "; recordings after restart=" + recordings);
                final Recording current = recordings.stream()
                        .max(Comparator.comparingLong(r -> r.id))
                        .orElseThrow();
                if (recordings.size() == 1) {
                    // The archive extended the SAME recording across the restart:
                    // the consensus module resumed at its previous position, so the
                    // recording is contiguous and still starts at 0. The first
                    // post-restart frame sits at or after the pre-restart end (a
                    // per-term header region may precede it), so frame positions
                    // never overlap the pre-restart data.
                    assertEquals(oldRecordingId, current.id, "the pre-restart recording is the one continued");
                    assertEquals(0L, current.startPos, "the continued recording still starts at position 0");
                    final List<Fragment> after = captureFrom(harness, archive, current.id, preRestartPosition);
                    assertFalse(after.isEmpty(), "the restarted recording must continue after the resume point");
                    assertTrue(
                            after.get(0).position >= preRestartPosition,
                            "the first post-restart frame must not overlap the pre-restart data; first="
                                    + after.get(0).position
                                    + " preRestartEnd="
                                    + preRestartPosition);
                } else {
                    final Recording old = recordings.stream()
                            .filter(r -> r.id == oldRecordingId)
                            .findFirst()
                            .orElse(null);
                    assertNotNull(old, "the pre-restart recording must still be listed");
                    assertTrue(current.id > oldRecordingId, "a newer recording must exist after the restart");
                    assertEquals(
                            old.stopPos,
                            current.startPos,
                            "the restarted member resumes its recording at the raft position where it stopped");
                }
            }
        } finally {
            if (node != null) {
                node.close();
            }
        }
    }

    // ------------------------------------------------------------------ helpers

    private static final long MAKER = 701L;
    private static final long TAKER = 702L;

    private static ClusterNode[] startNodes(final ClusterConfig[] configs) {
        final ClusterNode[] nodes = new ClusterNode[configs.length];
        for (int i = 0; i < configs.length; i++) {
            nodes[i] = new ClusterNode(configs[i], CoreConfig.defaults());
        }
        return nodes;
    }

    private static void closeNodes(final ClusterNode[] nodes) {
        for (final ClusterNode node : nodes) {
            if (node != null) {
                node.close();
            }
        }
    }

    private static String[] archiveChannels(final ClusterConfig[] configs) {
        final String[] channels = new String[configs.length];
        for (int i = 0; i < configs.length; i++) {
            channels[i] = configs[i].archiveControlChannel();
        }
        return channels;
    }

    private static void submitBatch(final Set<Long> results, final WriteClient client, final ClusterConfig[] configs) {
        awaitLeader(client);
        final int expected = 18;
        submit(client, () -> client.addSymbol(SYM, BASE, QUOTE, 1L, 1L));
        for (long uid = 1L; uid <= 5L; uid++) {
            final long u = uid;
            submit(client, () -> client.addUser(u));
            submit(client, () -> client.adjustBalance(u, BASE, 1_000L));
            submit(client, () -> client.adjustBalance(u, QUOTE, 1_000_000L));
        }
        submit(client, () -> client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, 1L, 0));
        submit(client, () -> client.placeGtc(SYM, 2L, false, 105L, 4L, 105L, 2L, 0));
        drainUntil(client, results, expected);
    }

    private static void submitBatch1(final WriteClient client, final long[] lastIdLo, final ReadReplica replica) {
        await(submit(client, () -> client.addSymbol(SYM, BASE, QUOTE, 1L, 1L)), client, replica, lastIdLo);
        for (long uid = 1L; uid <= 5L; uid++) {
            final long u = uid;
            await(submit(client, () -> client.addUser(u)), client, replica, lastIdLo);
            await(submit(client, () -> client.adjustBalance(u, BASE, 1_000L)), client, replica, lastIdLo);
            await(submit(client, () -> client.adjustBalance(u, QUOTE, 1_000_000L)), client, replica, lastIdLo);
        }
        await(submit(client, () -> client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, 1L, 0)), client, replica, lastIdLo);
        await(submit(client, () -> client.placeGtc(SYM, 2L, false, 105L, 4L, 105L, 2L, 0)), client, replica, lastIdLo);
    }

    private static void submitBatch2(final WriteClient client, final long[] lastIdLo, final ReadReplica replica) {
        for (long uid = 6L; uid <= 10L; uid++) {
            final long u = uid;
            await(submit(client, () -> client.addUser(u)), client, replica, lastIdLo);
            await(submit(client, () -> client.adjustBalance(u, BASE, 1_000L)), client, replica, lastIdLo);
            await(submit(client, () -> client.adjustBalance(u, QUOTE, 1_000_000L)), client, replica, lastIdLo);
        }
        await(submit(client, () -> client.placeGtc(SYM, 3L, true, 100L, 5L, 0L, 6L, 0)), client, replica, lastIdLo);
        await(submit(client, () -> client.placeGtc(SYM, 4L, false, 100L, 2L, 100L, 7L, 0)), client, replica, lastIdLo);
    }

    private static void awaitResult(final WriteClient client, final long commandIdLo, final long[] lastIdLo) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (lastIdLo[0] == commandIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no result for commandIdLo=" + commandIdLo);
    }

    private static void await(
            final long commandIdLo, final WriteClient client, final ReadReplica replica, final long[] lastIdLo) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            replica.poll();
            if (lastIdLo[0] == commandIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no result for command id " + commandIdLo);
    }

    private static void pollUntil(
            final WriteClient client, final ReadReplica replica, final BooleanSupplier condition) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            replica.poll();
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("replica never reached the expected state");
    }

    private static long submit(final WriteClient client, final LongSupplier op) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                return op.getAsLong();
            } catch (final BackpressureException e) {
                client.poll();
                Thread.onSpinWait();
            }
        }
        throw new AssertionError("could not submit command within timeout");
    }

    private static void drainUntil(final WriteClient client, final Set<Long> results, final int target) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (results.size() >= target) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("only " + results.size() + " of " + target + " results within timeout");
    }

    private static void awaitLeader(final WriteClient client) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (client.leaderMemberId() >= 0) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no leader established within timeout");
    }

    private static List<Recording> listStreamRecordings(final AeronArchive archive, final int streamId) {
        final List<Recording> out = new ArrayList<>();
        listRecordings(archive, (id, startPos, stopPos, stream) -> {
            if (stream == streamId) {
                out.add(new Recording(id, startPos, stopPos, stream));
            }
        });
        return out;
    }

    private static List<Recording> listAllRecordings(final AeronArchive archive) {
        final List<Recording> out = new ArrayList<>();
        listRecordings(
                archive, (id, startPos, stopPos, stream) -> out.add(new Recording(id, startPos, stopPos, stream)));
        return out;
    }

    @FunctionalInterface
    private interface RecordingSink {
        void accept(long id, long startPos, long stopPos, int streamId);
    }

    private static void listRecordings(final AeronArchive archive, final RecordingSink sink) {
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
                        sourceIdentity) -> sink.accept(recordingId, startPosition, stopPosition, streamId);
        archive.listRecordings(0L, 10_000, consumer);
    }

    private static Recording latestConsensusRecording(final AeronArchive archive) {
        Recording latest = null;
        for (final Recording recording : listStreamRecordings(archive, CONSENSUS_LOG_STREAM_ID)) {
            if (latest == null || recording.id > latest.id) {
                latest = recording;
            }
        }
        assertNotNull(latest, "a consensus log recording must exist on the archive");
        return latest;
    }

    private static List<Fragment> captureFrom(
            final Harness harness, final AeronArchive archive, final long recordingId, final long from) {
        final int streamId = harness.nextReplayStreamId();
        final List<Fragment> fragments = new ArrayList<>();
        try (Subscription subscription = harness.aeron.addSubscription("aeron:udp?endpoint=localhost:0", streamId)) {
            final String endpoint = awaitResolvedEndpoint(subscription);
            archive.startReplay(
                    recordingId, from, AeronArchive.NULL_LENGTH, "aeron:udp?endpoint=" + endpoint, streamId);
            long lastProgress = System.currentTimeMillis();
            final long deadline = System.currentTimeMillis() + CAPTURE_TIMEOUT_MS;
            while (System.currentTimeMillis() < deadline) {
                final int consumed = subscription.poll(
                        (buffer, offset, length, header) -> {
                            final byte[] payload = new byte[length];
                            buffer.getBytes(offset, payload, 0, length);
                            fragments.add(new Fragment(header.position(), length, payload));
                        },
                        64);
                if (consumed > 0) {
                    lastProgress = System.currentTimeMillis();
                } else if (System.currentTimeMillis() - lastProgress > QUIESCENCE_MS
                        || (subscription.imageCount() == 0 && !fragments.isEmpty())) {
                    break;
                }
                Thread.onSpinWait();
            }
        }
        return fragments;
    }

    private static SnapshotRecording awaitSnapshotRecording(final Harness harness, final AeronArchive archive) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        SnapshotRecording found = null;
        while (System.currentTimeMillis() < deadline && found == null) {
            found = sniffSnapshotRecording(harness, archive);
            if (found == null) {
                Thread.onSpinWait();
            }
        }
        return found;
    }

    /** Replays the newest recording of every stream and sniffs for the service SnapshotHeader. */
    private static SnapshotRecording sniffSnapshotRecording(final Harness harness, final AeronArchive archive) {
        final List<Recording> all = listAllRecordings(archive);
        all.sort(Comparator.comparingLong(r -> -r.id));
        for (final Recording recording : all) {
            if (recording.streamId == CONSENSUS_LOG_STREAM_ID) {
                continue;
            }
            final List<Fragment> head = captureFrom(harness, archive, recording.id, recording.startPos);
            final long logPosition = decodeSnapshotLogPosition(head);
            if (logPosition >= 0L) {
                return new SnapshotRecording(recording.id, recording.streamId, logPosition);
            }
        }
        return null;
    }

    private static long decodeSnapshotLogPosition(final List<Fragment> fragments) {
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        final SnapshotHeaderDecoder snapshotHeader = new SnapshotHeaderDecoder();
        for (final Fragment fragment : fragments) {
            final UnsafeBuffer buffer = new UnsafeBuffer(fragment.payload);
            header.wrap(buffer, 0);
            if (header.schemaId() == SnapshotHeaderDecoder.SCHEMA_ID
                    && header.templateId() == SnapshotHeaderDecoder.TEMPLATE_ID) {
                snapshotHeader.wrap(
                        buffer, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
                return snapshotHeader.logPosition();
            }
        }
        return -1L;
    }

    private static long replayStateHash(final AeronArchive archive, final long startPosition) {
        final CoreConfig core = CoreConfig.defaults();
        final MatchingEngine engine = new MatchingEngine(core, new CoreMetrics());
        final CommandOutcome outcome = new CommandOutcome(core.eventBufferCapacity());
        final LiveLogSubscriber subscriber = new LiveLogSubscriber(
                archive, engine, outcome, new OrderLedger(), ReplicaCommandListener.NONE, startPosition, "localhost");
        assertTrue(subscriber.connect(), "the live log replay must connect");
        try {
            final long deadline = System.currentTimeMillis() + REPLAY_TIMEOUT_MS;
            while (System.currentTimeMillis() < deadline) {
                subscriber.poll(64);
                if (subscriber.isCaughtUp()) {
                    final long quiescence = System.currentTimeMillis() + 500L;
                    while (System.currentTimeMillis() < quiescence) {
                        subscriber.poll(64);
                        Thread.onSpinWait();
                    }
                    return new ReportGenerator(engine).stateHash();
                }
                Thread.onSpinWait();
            }
            throw new AssertionError("replay from position " + startPosition + " never caught up");
        } finally {
            subscriber.close();
        }
    }

    /**
     * Replays the recording up to and including the frame at {@code prefixEnd},
     * then resumes a second replay from that same position, so the engine's final
     * state is the prefix state plus the new source's tail. This models the Phase
     * 1 failover path: keep the state built from the old source and continue from
     * the committed boundary on the new source. The boundary frame is re-applied
     * at most once and absorbed by engine dedup, and the resume position is a
     * multiple of the archive's FRAME_ALIGNMENT.
     */
    private static long resumeStateHash(final AeronArchive archive, final long prefixEnd) {
        final CoreConfig core = CoreConfig.defaults();
        final MatchingEngine engine = new MatchingEngine(core, new CoreMetrics());
        final CommandOutcome outcome = new CommandOutcome(core.eventBufferCapacity());

        final LiveLogSubscriber prefix = new LiveLogSubscriber(
                archive, engine, outcome, new OrderLedger(), ReplicaCommandListener.NONE, 0L, "localhost");
        assertTrue(prefix.connect(), "the prefix replay must connect");
        try {
            final long deadline = System.currentTimeMillis() + REPLAY_TIMEOUT_MS;
            while (System.currentTimeMillis() < deadline && prefix.lastPosition() < prefixEnd) {
                prefix.poll(1);
            }
            assertEquals(
                    prefixEnd,
                    prefix.lastPosition(),
                    "the prefix replay must consume the frame at the committed boundary");
        } finally {
            prefix.close();
        }

        final LiveLogSubscriber tail = new LiveLogSubscriber(
                archive, engine, outcome, new OrderLedger(), ReplicaCommandListener.NONE, prefixEnd, "localhost");
        assertTrue(tail.connect(), "the tail replay must connect");
        try {
            final long deadline = System.currentTimeMillis() + REPLAY_TIMEOUT_MS;
            while (System.currentTimeMillis() < deadline) {
                tail.poll(64);
                if (tail.isCaughtUp()) {
                    final long quiescence = System.currentTimeMillis() + 500L;
                    while (System.currentTimeMillis() < quiescence) {
                        tail.poll(64);
                        Thread.onSpinWait();
                    }
                    return new ReportGenerator(engine).stateHash();
                }
                Thread.onSpinWait();
            }
            throw new AssertionError("the tail replay never caught up");
        } finally {
            tail.close();
        }
    }

    private static void assertStrictlyIncreasing(final List<Fragment> fragments) {
        for (int i = 1; i < fragments.size(); i++) {
            assertTrue(
                    fragments.get(i).position > fragments.get(i - 1).position,
                    "fragment positions must strictly increase");
        }
    }

    /** The first {@code n} positions of {@code fragments}, for diagnostics. */
    private static String positions(final List<Fragment> fragments, final int n) {
        final StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < Math.min(n, fragments.size()); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(fragments.get(i).position);
        }
        return sb.append("]").toString();
    }

    /** The last {@code n} fragments. */
    private static List<Fragment> tail(final List<Fragment> fragments, final int n) {
        final int from = Math.max(0, fragments.size() - n);
        return new ArrayList<>(fragments.subList(from, fragments.size()));
    }

    /**
     * The position of the last fragment of the longest common prefix of two
     * recordings, or -1 when they diverge immediately. The committed prefix is
     * identical on every member, so this is the highest position that is safe to
     * resume from on either member.
     */
    private static long commonPrefixEndPosition(final List<Fragment> a, final List<Fragment> b) {
        final int common = Math.min(a.size(), b.size());
        for (int i = 0; i < common; i++) {
            final Fragment fa = a.get(i);
            final Fragment fb = b.get(i);
            if (fa.position != fb.position
                    || fa.length != fb.length
                    || !java.util.Arrays.equals(fa.payload, fb.payload)) {
                return i == 0 ? -1L : a.get(i - 1).position;
            }
        }
        return common == 0 ? -1L : a.get(common - 1).position;
    }

    private static String awaitResolvedEndpoint(final Subscription subscription) {
        final long deadline = System.currentTimeMillis() + CAPTURE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            final String endpoint = subscription.resolvedEndpoint();
            if (endpoint != null) {
                return endpoint;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("replay endpoint did not resolve within timeout");
    }
}
