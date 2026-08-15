package com.exadbe.read;

import com.exadbe.config.CoreConfig;
import com.exadbe.protocol.QueryStreams;
import com.exadbe.read.config.ReadReplicaConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

/**
 * Launches a standalone read replica that follows a cluster member's archive.
 *
 * <pre>{@code
 * ./gradlew :exc-read:run --args="--archive=aeron:udp?endpoint=localhost:20104"
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
                default -> throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }

        final String[] channels = archiveControlChannels.split(",");
        final String aeronDir =
                Files.createTempDirectory("exc-read-").resolve("driver").toString();
        final ReadReplicaConfig.Builder builder = ReadReplicaConfig.builder(aeronDir)
                .channels(channels)
                .localHost(host)
                .query(queryChannel, QueryStreams.QUERY_REQUEST_STREAM_ID);
        if (checkpoint != null && !checkpoint.isBlank()) {
            builder.checkpointFile(Path.of(checkpoint));
        }
        final ReadReplicaConfig config = builder.build();

        final IdleStrategy idle = new BackoffIdleStrategy();
        try (ExcReadReplica replica = new ExcReadReplica(config, CoreConfig.defaults());
                QueryResponder responder = new QueryResponder(replica, config)) {
            System.out.println("exc-read replica following " + archiveControlChannels + ", serving queries on "
                    + config.queryRequestChannel() + " (Ctrl-C to stop)");
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    final int work = replica.poll() + responder.poll();
                    idle.idle(work);
                } catch (final RuntimeException e) {
                    // A transient failure must not kill the read service: the
                    // replica keeps serving its last known state and retries.
                    e.printStackTrace();
                    idle.idle();
                }
            }
        }
    }
}
