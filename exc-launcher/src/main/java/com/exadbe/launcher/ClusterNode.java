package com.exadbe.launcher;

import com.exadbe.config.CoreConfig;
import com.exadbe.core.MatchingService;
import com.exadbe.telemetry.CoreMetrics;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

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

    private final ClusteredMediaDriver clusteredMediaDriver;
    private final ClusteredServiceContainer container;
    private final CoreMetrics metrics;

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
        this.metrics = new CoreMetrics();

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
    }

    public CoreMetrics metrics() {
        return metrics;
    }

    @Override
    public void close() {
        if (container != null) {
            container.close();
        }
        if (clusteredMediaDriver != null) {
            clusteredMediaDriver.close();
        }
    }
}
