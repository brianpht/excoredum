package io.justrade.bench;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.read.QueryResponder;
import io.justrade.read.ReadReplica;
import io.justrade.read.config.ReadReplicaConfig;
import java.nio.file.Path;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end system test in one JVM: runs the deterministic
 * {@link LoadWorkload} through the write-side {@link ExternalLoadRunner}
 * against an in-process single-node cluster, then verifies the read side
 * through {@link ReadVerifyRunner} against an in-process read replica
 * following the node's archive. This is the same pipeline the dockerized
 * system test runs, without containers, and cross-validates the workload
 * simulation against the real engine (matching order, reserve/release
 * semantics, ledger counts).
 */
@Tag("integration")
class SystemLoadIntegrationTest {

    @Test
    @Timeout(600)
    void writeLoadAndReadVerifyMatchSimulation(@TempDir final Path baseDir) throws Exception {
        final int ops = Integer.getInteger("justrade.systemload.ops", 20_000);
        final int users = Integer.getInteger("justrade.systemload.users", 20);
        final LoadWorkload workload = new LoadWorkload(ops, users);
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);

        try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults())) {
            assertTrue(
                    ExternalLoadRunner.run(
                            workload, ClusterConfig.ingressEndpoints(1), "aeron:udp?endpoint=localhost:0"),
                    "write-side load must match the simulation");

            final ReadReplicaConfig replicaConfig = ReadReplicaConfig.localhost(
                    baseDir.resolve("replica").resolve("driver").toString(), clusterConfig.archiveControlChannel());
            try (ReadReplica replica = new ReadReplica(replicaConfig, CoreConfig.defaults());
                    QueryResponder responder = new QueryResponder(replica, replicaConfig)) {
                final Thread serviceThread = startServiceLoop(replica, responder);
                try {
                    assertTrue(
                            ReadVerifyRunner.verify(
                                    workload, "aeron:udp?endpoint=localhost:44000", "aeron:udp?endpoint=localhost:0"),
                            "read-side state must match the simulation");
                } finally {
                    serviceThread.interrupt();
                    serviceThread.join(5_000L);
                }
            }
        }
    }

    private static Thread startServiceLoop(final ReadReplica replica, final QueryResponder responder) {
        final Thread thread = new Thread(() -> {
            final BackoffIdleStrategy idle = new BackoffIdleStrategy();
            while (!Thread.currentThread().isInterrupted()) {
                final int work = replica.poll() + responder.poll();
                idle.idle(work);
            }
        });
        thread.setDaemon(true);
        thread.setName("system-load-read-service");
        thread.start();
        return thread;
    }
}
