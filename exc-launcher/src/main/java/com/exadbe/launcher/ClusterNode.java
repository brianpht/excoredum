package com.exadbe.launcher;

import com.exadbe.config.CoreConfig;
import com.exadbe.core.MatchingService;
import com.exadbe.journal.JournalStreams;
import com.exadbe.telemetry.AtomicCounterSink;
import com.exadbe.telemetry.CoreMetrics;
import com.exadbe.telemetry.CounterSink;
import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.util.Locale;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

/**
 * Launches and owns the Aeron components for one cluster node: the clustered
 * media driver (Media Driver + Archive + Consensus Module) and a single
 * {@link ClusteredServiceContainer} hosting one clustered service.
 *
 * <p>A single service agent runs the matching logic on one thread, satisfying
 * the single-writer / no-locks requirement.
 */
public final class ClusterNode implements AutoCloseable {

    /** Creates the {@link ClusteredService} hosted by this node. */
    @FunctionalInterface
    public interface ClusteredServiceFactory {
        ClusteredService create(CoreConfig config, CoreMetrics metrics);
    }

    /** IPC channel the domain-event journal is published and recorded on. */
    public static final String JOURNAL_CHANNEL = JournalStreams.JOURNAL_CHANNEL;

    /** Stream id of the recorded domain-event journal. */
    public static final int JOURNAL_STREAM_ID = JournalStreams.JOURNAL_STREAM_ID;

    private static final int JOURNAL_FRAGMENT_LIMIT = 64;

    private final ClusteredMediaDriver clusteredMediaDriver;
    private final ClusteredServiceContainer container;
    private final CountersManager countersManager;
    private final CoreMetrics metrics;
    private final Aeron journalAeron;
    private final AeronArchive journalArchive;
    private final ExclusivePublication journalPublication;
    private final AgentRunner journalRunner;
    private final EventJournalRecorder journalRecorder;

    /** Launches a node that clears prior state on start (fresh cluster). */
    public ClusterNode(final ClusterConfig config, final CoreConfig coreConfig) {
        this(config, coreConfig, true);
    }

    /**
     * Launches a node.
     *
     * @param cleanStart when {@code true}, deletes any prior archive and cluster
     *     directories on start (fresh cluster). When {@code false}, preserves
     *     them so the node can recover its log and catch up after a restart.
     */
    public ClusterNode(final ClusterConfig config, final CoreConfig coreConfig, final boolean cleanStart) {
        this(config, coreConfig, cleanStart, MatchingService::new);
    }

    /**
     * Launches a node hosting the service produced by {@code serviceFactory}.
     *
     * @param cleanStart when {@code true}, deletes any prior archive and cluster
     *     directories on start (fresh cluster).
     * @param serviceFactory builds the clustered service this node hosts.
     */
    public ClusterNode(
            final ClusterConfig config,
            final CoreConfig coreConfig,
            final boolean cleanStart,
            final ClusteredServiceFactory serviceFactory) {
        this.countersManager = newCountersManager();
        this.metrics = new CoreMetrics(
                new AtomicCounterSink(allocateCounters(countersManager), allocateGauges(countersManager)));

        final String localControlChannel = "aeron:ipc?term-length=64k";

        final AeronArchive.Context archiveClientContext = new AeronArchive.Context()
                .aeronDirectoryName(config.aeronDirectoryName())
                .controlRequestChannel(localControlChannel)
                .controlResponseChannel(localControlChannel);

        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
                .aeronDirectoryName(config.aeronDirectoryName())
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        final Archive.Context archiveContext = new Archive.Context()
                .aeronDirectoryName(config.aeronDirectoryName())
                .archiveDir(config.archiveDir())
                .controlChannel(config.archiveControlChannel())
                .localControlChannel(localControlChannel)
                .replicationChannel(config.replicationChannel())
                .recordingEventsEnabled(false)
                .deleteArchiveOnStart(cleanStart);

        final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context()
                .clusterMemberId(config.nodeId())
                .clusterMembers(config.clusterMembers())
                .aeronDirectoryName(config.aeronDirectoryName())
                .clusterDir(config.clusterDir())
                .ingressChannel(config.ingressChannel())
                .replicationChannel(config.replicationChannel())
                .archiveContext(archiveClientContext.clone())
                .deleteDirOnStart(cleanStart);

        final ClusteredService service = serviceFactory.create(coreConfig, metrics);
        final ClusteredServiceContainer.Context serviceContext = new ClusteredServiceContainer.Context()
                .aeronDirectoryName(config.aeronDirectoryName())
                .archiveContext(archiveClientContext.clone())
                .clusterDir(config.clusterDir())
                .clusteredService(service);

        this.clusteredMediaDriver =
                ClusteredMediaDriver.launch(mediaDriverContext, archiveContext, consensusModuleContext);
        try {
            this.container = ClusteredServiceContainer.launch(serviceContext);
        } catch (final RuntimeException e) {
            // Do not leak the media driver (and its non-daemon agent threads) if
            // the service container fails to start.
            clusteredMediaDriver.close();
            throw e;
        }

        // Drain the service's domain-event ring to a recorded journal stream on a
        // dedicated thread; the Archive is the always-on consumer so the recorder
        // never stalls and the consensus thread is never touched by journal I/O.
        Aeron aeronClient = null;
        AeronArchive archiveClient = null;
        ExclusivePublication publication = null;
        AgentRunner runner = null;
        EventJournalRecorder eventRecorder = null;
        if (service instanceof MatchingService matchingService) {
            try {
                aeronClient = Aeron.connect(new Aeron.Context().aeronDirectoryName(config.aeronDirectoryName()));
                archiveClient = AeronArchive.connect(new AeronArchive.Context()
                        .aeron(aeronClient)
                        .controlRequestChannel(localControlChannel)
                        .controlResponseChannel(localControlChannel));
                archiveClient.startRecording(JOURNAL_CHANNEL, JOURNAL_STREAM_ID, SourceLocation.LOCAL);
                publication = aeronClient.addExclusivePublication(JOURNAL_CHANNEL, JOURNAL_STREAM_ID);
                eventRecorder = new EventJournalRecorder(
                        matchingService.journal(), publication, new BackoffIdleStrategy(), JOURNAL_FRAGMENT_LIMIT);
                runner = new AgentRunner(new BackoffIdleStrategy(), Throwable::printStackTrace, null, eventRecorder);
                AgentRunner.startOnThread(runner);
            } catch (final RuntimeException e) {
                CloseHelper.quietClose(runner);
                CloseHelper.quietClose(publication);
                CloseHelper.quietClose(archiveClient);
                CloseHelper.quietClose(aeronClient);
                container.close();
                clusteredMediaDriver.close();
                throw e;
            }
        }
        this.journalAeron = aeronClient;
        this.journalArchive = archiveClient;
        this.journalPublication = publication;
        this.journalRunner = runner;
        this.journalRecorder = eventRecorder;
    }

    public CoreMetrics metrics() {
        return metrics;
    }

    /** Total domain events published to the recorded journal stream so far. */
    public long journalPublished() {
        return journalRecorder == null ? 0L : journalRecorder.published();
    }

    /** The off-heap counters manager backing the core metrics for this node. */
    public CountersManager countersManager() {
        return countersManager;
    }

    @Override
    public void close() {
        CloseHelper.quietClose(journalRunner);
        CloseHelper.quietClose(journalPublication);
        CloseHelper.quietClose(journalArchive);
        CloseHelper.quietClose(journalAeron);
        if (container != null) {
            container.close();
        }
        if (clusteredMediaDriver != null) {
            clusteredMediaDriver.close();
        }
    }

    private static CountersManager newCountersManager() {
        final int maxCounters = CounterSink.Counter.COUNT + CounterSink.Gauge.COUNT;
        final UnsafeBuffer valuesBuffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(
                maxCounters * CountersManager.COUNTER_LENGTH, BitUtil.CACHE_LINE_LENGTH));
        final UnsafeBuffer metadataBuffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(
                maxCounters * CountersManager.METADATA_LENGTH, BitUtil.CACHE_LINE_LENGTH));
        return new CountersManager(metadataBuffer, valuesBuffer);
    }

    private static AtomicCounter[] allocateCounters(final CountersManager countersManager) {
        final CounterSink.Counter[] all = CounterSink.Counter.values();
        final AtomicCounter[] counters = new AtomicCounter[all.length];
        for (final CounterSink.Counter counter : all) {
            counters[counter.ordinal()] = countersManager.newCounter(
                    "exc." + counter.name().toLowerCase(Locale.ROOT), CounterSink.TYPE_COUNTER);
        }
        return counters;
    }

    private static AtomicCounter[] allocateGauges(final CountersManager countersManager) {
        final CounterSink.Gauge[] all = CounterSink.Gauge.values();
        final AtomicCounter[] gauges = new AtomicCounter[all.length];
        for (final CounterSink.Gauge gauge : all) {
            gauges[gauge.ordinal()] =
                    countersManager.newCounter("exc." + gauge.name().toLowerCase(Locale.ROOT), CounterSink.TYPE_GAUGE);
        }
        return gauges;
    }
}
