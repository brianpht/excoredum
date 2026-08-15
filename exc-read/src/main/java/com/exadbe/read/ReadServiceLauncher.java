package com.exadbe.read;

import com.exadbe.config.CoreConfig;
import com.exadbe.protocol.QueryStreams;
import com.exadbe.read.config.ReadReplicaConfig;
import java.nio.file.Files;
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
 */
public final class ReadServiceLauncher {

    private ReadServiceLauncher() {}

    public static void main(final String[] args) throws Exception {
        String archiveControlChannel = "aeron:udp?endpoint=localhost:20104";
        String host = "localhost";
        String queryChannel = QueryStreams.QUERY_REQUEST_CHANNEL;
        for (final String arg : args) {
            final int eq = arg.indexOf('=');
            if (!arg.startsWith("--") || eq < 0) {
                throw new IllegalArgumentException("unknown argument: " + arg);
            }
            switch (arg.substring(0, eq)) {
                case "--archive" -> archiveControlChannel = arg.substring(eq + 1);
                case "--host" -> host = arg.substring(eq + 1);
                case "--query" -> queryChannel = arg.substring(eq + 1);
                default -> throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }

        final String aeronDir =
                Files.createTempDirectory("exc-read-").resolve("driver").toString();
        final ReadReplicaConfig config = ReadReplicaConfig.localhost(
                aeronDir, archiveControlChannel, host, queryChannel, QueryStreams.QUERY_REQUEST_STREAM_ID);

        final IdleStrategy idle = new BackoffIdleStrategy();
        try (ExcReadReplica replica = new ExcReadReplica(config, CoreConfig.defaults());
                QueryResponder responder = new QueryResponder(replica, config)) {
            System.out.println("exc-read replica following " + archiveControlChannel + ", serving queries on "
                    + config.queryRequestChannel() + " (Ctrl-C to stop)");
            while (!Thread.currentThread().isInterrupted()) {
                final int work = replica.poll() + responder.poll();
                idle.idle(work);
            }
        }
    }
}
