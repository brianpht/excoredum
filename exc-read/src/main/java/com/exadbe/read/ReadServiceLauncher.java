package com.exadbe.read;

import com.exadbe.config.CoreConfig;
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
 */
public final class ReadServiceLauncher {

    private ReadServiceLauncher() {}

    public static void main(final String[] args) throws Exception {
        String archiveControlChannel = "aeron:udp?endpoint=localhost:20104";
        for (final String arg : args) {
            final int eq = arg.indexOf('=');
            if (arg.startsWith("--archive=") && eq >= 0) {
                archiveControlChannel = arg.substring(eq + 1);
            } else {
                throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }

        final String aeronDir =
                Files.createTempDirectory("exc-read-").resolve("driver").toString();
        final ReadReplicaConfig config = ReadReplicaConfig.localhost(aeronDir, archiveControlChannel);

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
