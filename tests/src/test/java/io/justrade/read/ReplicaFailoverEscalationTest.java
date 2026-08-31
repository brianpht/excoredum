package io.justrade.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.read.config.ReadReplicaConfig;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Error-driven failovers that never advance the applied position must escalate
 * into a rebuild from the log start instead of cycling sources forever: every
 * member replays the same committed prefix, so a fragment that throws on one
 * source throws on all of them.
 */
final class ReplicaFailoverEscalationTest {

    @Test
    void repeatedFailoversWithoutProgressEscalateToRebuild(@TempDir final Path baseDir) {
        try (ReadReplica replica = newReplica(baseDir)) {
            for (int i = 0; i < 7; i++) {
                assertFalse(replica.failoverWithoutProgress(100L), "no escalation before the threshold");
            }
            assertTrue(replica.failoverWithoutProgress(100L), "the threshold-th stale failover rebuilds");
            assertEquals(0L, replica.appliedPosition(), "the rebuild resets the applied position");
            assertEquals(1L, replica.health().rebuildFailures(), "the escalation is observable");

            // The escalation is repeatable: the counter resets after firing.
            for (int i = 0; i < 7; i++) {
                assertFalse(replica.failoverWithoutProgress(200L));
            }
            assertTrue(replica.failoverWithoutProgress(200L));
            assertEquals(2L, replica.health().rebuildFailures());
        }
    }

    @Test
    void progressBetweenFailoversNeverEscalates(@TempDir final Path baseDir) {
        try (ReadReplica replica = newReplica(baseDir)) {
            for (long position = 1L; position <= 100L; position++) {
                assertFalse(replica.failoverWithoutProgress(position), "an advancing position resets the count");
            }
            assertEquals(0L, replica.health().rebuildFailures());
        }
    }

    private static ReadReplica newReplica(final Path baseDir) {
        final ReadReplicaConfig config = ReadReplicaConfig.builder(
                        baseDir.resolve("driver").toString())
                .channels("aeron:udp?endpoint=localhost:20999")
                .localHost("localhost")
                .build();
        return new ReadReplica(config, CoreConfig.defaults());
    }
}
