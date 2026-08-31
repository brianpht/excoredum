package io.justrade.read;

import io.justrade.config.CoreConfig;
import io.justrade.protocol.QueryStreams;
import io.justrade.read.config.ReadReplicaConfig;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

/**
 * Launches a standalone read replica that follows a cluster member's archive.
 *
 * <pre>{@code
 * ./gradlew :read:run --args="--archive=aeron:udp?endpoint=localhost:20104"
 * }</pre>
 *
 * <p>Container deployments override {@code --host} (the address the replica
 * binds for archive control responses and log-replay, default
 * {@code localhost}) and {@code --query} (the channel the replica listens on
 * for read-side queries, default {@code aeron:udp?endpoint=localhost:44000}).
 * {@code --archive} accepts a comma-separated list of member archive control
 * channels; the first is the primary source and the replica fails over to the
 * rest (in order) when it dies, resuming from the applied position.
 *
 * <p>{@code --checkpoint} enables local checkpoint persistence (engine + ledger
 * + position) for fast warm restarts. A transient poll failure degrades the
 * replica (it keeps serving its last state) instead of killing the process.
 */
public final class ReadServiceLauncher {

    private ReadServiceLauncher() {}

    public static void main(final String[] args) throws Exception {
        String archiveControlChannels = "aeron:udp?endpoint=localhost:20104";
        String host = "localhost";
        String queryChannel = QueryStreams.QUERY_REQUEST_CHANNEL;
        String checkpoint = null;
        String coreConfigPath = null;
        int maxOrdersPerUser = -1;
        int maxMarketTrades = -1;
        for (final String arg : args) {
            final int eq = arg.indexOf('=');
            if (!arg.startsWith("--") || eq < 0) {
                throw new IllegalArgumentException("unknown argument: " + arg);
            }
            switch (arg.substring(0, eq)) {
                case "--archive" -> archiveControlChannels = arg.substring(eq + 1);
                case "--host" -> host = arg.substring(eq + 1);
                case "--query" -> queryChannel = arg.substring(eq + 1);
                case "--checkpoint" -> checkpoint = arg.substring(eq + 1);
                case "--core-config" -> coreConfigPath = arg.substring(eq + 1);
                case "--ledger-max-orders-per-user" -> maxOrdersPerUser = Integer.parseInt(arg.substring(eq + 1));
                case "--ledger-max-market-trades" -> maxMarketTrades = Integer.parseInt(arg.substring(eq + 1));
                default -> throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }

        final CoreConfig coreConfig = loadCoreConfig(coreConfigPath);
        final String[] channels = archiveControlChannels.split(",");
        final String aeronDir =
                Files.createTempDirectory("read-").resolve("driver").toString();
        final ReadReplicaConfig.Builder builder = ReadReplicaConfig.builder(aeronDir)
                .channels(channels)
                .localHost(host)
                .query(queryChannel, QueryStreams.QUERY_REQUEST_STREAM_ID);
        if (checkpoint != null && !checkpoint.isBlank()) {
            builder.checkpointFile(Path.of(checkpoint));
        }
        if (maxOrdersPerUser > 0) {
            builder.maxOrdersPerUser(maxOrdersPerUser);
        }
        if (maxMarketTrades > 0) {
            builder.maxMarketTrades(maxMarketTrades);
        }
        final ReadReplicaConfig config = builder.build();

        final IdleStrategy idle = new BackoffIdleStrategy();
        long pollFailures = 0L;
        try (ExcReadReplica replica = new ExcReadReplica(config, coreConfig);
                QueryResponder responder = new QueryResponder(replica, config)) {
            System.out.println("read replica following " + archiveControlChannels + ", serving queries on "
                    + config.queryRequestChannel() + " (Ctrl-C to stop)");
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    final int work = replica.poll() + responder.poll();
                    idle.idle(work);
                } catch (final RuntimeException e) {
                    // A transient failure must not kill the read service: the
                    // replica keeps serving its last known state and retries.
                    // Report the first failure and then every 1024th so a
                    // flapping source cannot spam the console with stack traces.
                    if (pollFailures == 0L || pollFailures >= 1024L) {
                        System.err.println("read poll failed (" + pollFailures + " total): " + e);
                    }
                    pollFailures++;
                    idle.idle();
                }
            }
        }
    }

    /**
     * Builds the core configuration: a {@code --core-config} properties file
     * takes precedence, then {@code -Djustrade.core.*} system properties, then the
     * compiled defaults.
     */
    private static CoreConfig loadCoreConfig(final String coreConfigPath) {
        if (coreConfigPath != null && !coreConfigPath.isBlank()) {
            final Properties props = new Properties();
            try (InputStream in = Files.newInputStream(Path.of(coreConfigPath))) {
                props.load(in);
            } catch (final IOException e) {
                throw new UncheckedIOException("Failed to read core config: " + coreConfigPath, e);
            }
            return CoreConfig.fromProperties(props);
        }
        return CoreConfig.fromSystemProperties();
    }
}
