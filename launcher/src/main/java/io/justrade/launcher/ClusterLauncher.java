package io.justrade.launcher;

import io.justrade.config.CoreConfig;
import io.justrade.telemetry.CoreMetrics;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;

/**
 * Entry point that starts a single justrade cluster node and blocks until the
 * JVM is terminated.
 *
 * <p>Configuration sources, in order of precedence:
 *
 * <ul>
 *   <li>{@code --config=<file>} argument or {@code -Djustrade.config=<file>}: load a
 *       node from a {@code .properties} file (production / multi-node).
 *   <li>otherwise a single-node localhost cluster is started, with node id and
 *       base directory from {@code -Djustrade.nodeId} and {@code -Djustrade.baseDir}.
 * </ul>
 *
 * <p>{@code -Djustrade.cleanStart=false} preserves prior archive and cluster state so
 * a restarted node can recover and catch up.
 */
public final class ClusterLauncher {

    private ClusterLauncher() {}

    public static void main(final String[] args) {
        final int nodeId = Integer.getInteger("justrade.nodeId", 0);
        final String configPath = configPath(args);

        final ClusterConfig clusterConfig;
        final CoreConfig coreConfig;
        if (configPath != null) {
            final Properties props = loadProperties(configPath);
            clusterConfig = ClusterConfig.fromProperties(props, nodeId);
            coreConfig = CoreConfig.fromProperties(props);
        } else {
            final Path baseDir = Paths.get(System.getProperty("justrade.baseDir", "build/justrade-node-" + nodeId));
            clusterConfig = ClusterConfig.singleNodeLocalhost(nodeId, baseDir);
            coreConfig = CoreConfig.fromSystemProperties();
        }

        final boolean cleanStart = Boolean.parseBoolean(System.getProperty("justrade.cleanStart", "true"));

        final ClusterNode node = new ClusterNode(clusterConfig, coreConfig, cleanStart);
        Runtime.getRuntime().addShutdownHook(new Thread(node::close, "shutdown"));

        startMetricsDump(node);

        // Park the main thread; the service runs on the clustered service agent thread.
        while (!Thread.currentThread().isInterrupted()) {
            LockSupport.parkNanos(1_000_000_000L);
        }
    }

    /**
     * Optionally starts a daemon thread that writes this node's {@link CoreMetrics}
     * to stdout every {@code justrade.metricsIntervalMs} milliseconds, so a deployed
     * node's internal counters are observable from its logs without a query
     * channel. Disabled by default ({@code interval <= 0}).
     *
     * <p>The counters are plain {@code long} fields owned by the service thread,
     * so reads here are eventually consistent (monitoring only). This thread
     * never mutates engine state, so determinism and snapshots are unaffected.
     */
    private static void startMetricsDump(final ClusterNode node) {
        final long intervalMs = Long.getLong("justrade.metricsIntervalMs", 0L);
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
                "metrics-dump");
        thread.setDaemon(true);
        thread.start();
    }

    private static Properties loadProperties(final String configPath) {
        final Properties props = new Properties();
        try (InputStream in = Files.newInputStream(Paths.get(configPath))) {
            props.load(in);
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to read config: " + configPath, e);
        }
        return props;
    }

    private static String configPath(final String[] args) {
        final String prefix = "--config=";
        for (final String arg : args) {
            if (arg.startsWith(prefix)) {
                return arg.substring(prefix.length());
            }
        }
        return System.getProperty("justrade.config");
    }
}
