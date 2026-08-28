package com.exadbe.launcher;

import com.exadbe.config.CoreConfig;
import com.exadbe.telemetry.CoreMetrics;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.locks.LockSupport;

/**
 * Entry point that starts a single excoredum cluster node and blocks until the
 * JVM is terminated.
 *
 * <p>Configuration sources, in order of precedence:
 *
 * <ul>
 *   <li>{@code --config=<file>} argument or {@code -Dexc.config=<file>}: load a
 *       node from a {@code .properties} file (production / multi-node).
 *   <li>otherwise a single-node localhost cluster is started, with node id and
 *       base directory from {@code -Dexc.nodeId} and {@code -Dexc.baseDir}.
 * </ul>
 *
 * <p>{@code -Dexc.cleanStart=false} preserves prior archive and cluster state so
 * a restarted node can recover and catch up.
 */
public final class ClusterLauncher {

    private ClusterLauncher() {}

    public static void main(final String[] args) {
        final int nodeId = Integer.getInteger("exc.nodeId", 0);
        final String configPath = configPath(args);

        final ClusterConfig clusterConfig;
        if (configPath != null) {
            clusterConfig = ClusterConfig.fromProperties(Paths.get(configPath), nodeId);
        } else {
            final Path baseDir = Paths.get(System.getProperty("exc.baseDir", "build/exc-node-" + nodeId));
            clusterConfig = ClusterConfig.singleNodeLocalhost(nodeId, baseDir);
        }

        final boolean cleanStart = Boolean.parseBoolean(System.getProperty("exc.cleanStart", "true"));
        final CoreConfig coreConfig = CoreConfig.defaults();

        final ClusterNode node = new ClusterNode(clusterConfig, coreConfig, cleanStart);
        Runtime.getRuntime().addShutdownHook(new Thread(node::close, "exc-shutdown"));

        startMetricsDump(node);

        // Park the main thread; the service runs on the clustered service agent thread.
        while (!Thread.currentThread().isInterrupted()) {
            LockSupport.parkNanos(1_000_000_000L);
        }
    }

    /**
     * Optionally starts a daemon thread that writes this node's {@link CoreMetrics}
     * to stdout every {@code exc.metricsIntervalMs} milliseconds, so a deployed
     * node's internal counters are observable from its logs without a query
     * channel. Disabled by default ({@code interval <= 0}).
     *
     * <p>The counters are plain {@code long} fields owned by the service thread,
     * so reads here are eventually consistent (monitoring only). This thread
     * never mutates engine state, so determinism and snapshots are unaffected.
     */
    private static void startMetricsDump(final ClusterNode node) {
        final long intervalMs = Long.getLong("exc.metricsIntervalMs", 0L);
        if (intervalMs <= 0L) {
            return;
        }
        final CoreMetrics metrics = node.metrics();
        final Thread thread = new Thread(
                () -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        System.out.printf(
                                "metrics commands=%d duplicates=%d backpressure=%d unsupported=%d snapshotsTaken=%d"
                                        + " snapshotsLoaded=%d eventBufferOverflows=%d orderPoolExhaustions=%d"
                                        + " priceBucketPoolExhaustions=%d journalBackpressure=%d dedupEvictions=%d"
                                        + " snapshotWriteMs=%d snapshotReadMs=%d journalPublished=%d%n",
                                metrics.commandsProcessed(),
                                metrics.duplicates(),
                                metrics.backpressureEvents(),
                                metrics.unsupportedCommands(),
                                metrics.snapshotsTaken(),
                                metrics.snapshotsLoaded(),
                                metrics.eventBufferOverflows(),
                                metrics.orderPoolExhaustions(),
                                metrics.priceBucketPoolExhaustions(),
                                metrics.journalBackpressureEvents(),
                                metrics.dedupEvictions(),
                                metrics.lastSnapshotWriteMillis(),
                                metrics.lastSnapshotReadMillis(),
                                node.journalPublished());
                        try {
                            Thread.sleep(intervalMs);
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                },
                "exc-metrics-dump");
        thread.setDaemon(true);
        thread.start();
    }

    private static String configPath(final String[] args) {
        final String prefix = "--config=";
        for (final String arg : args) {
            if (arg.startsWith(prefix)) {
                return arg.substring(prefix.length());
            }
        }
        return System.getProperty("exc.config");
    }
}
