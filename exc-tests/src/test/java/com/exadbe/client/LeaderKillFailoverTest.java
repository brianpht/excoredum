package com.exadbe.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.client.config.ClientConfig;
import com.exadbe.config.CoreConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.protocol.CommandResultCode;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P5 chaos: a three-node cluster survives losing its leader. Commands submitted
 * before and after the kill each resolve to exactly one SUCCESS result - no loss
 * (every submission is acknowledged) and no duplication (idempotent retry plus
 * engine dedup means a re-applied command would surface as an error, never a
 * second SUCCESS).
 */
@Tag("fault")
class LeaderKillFailoverTest {

    private static final int NODES = 3;
    private static final long CONNECT_TIMEOUT_MS = 30_000L;
    private static final long DRAIN_TIMEOUT_MS = 90_000L;
    private static final int FIRST_BATCH = 20;
    private static final int SECOND_BATCH = 20;

    @Test
    @Timeout(240)
    void survivesLeaderKillWithoutLossOrDuplication(@TempDir final Path baseDir) {
        final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODES, baseDir);
        final ClusterNode[] nodes = new ClusterNode[NODES];
        for (int i = 0; i < NODES; i++) {
            nodes[i] = new ClusterNode(configs[i], CoreConfig.defaults());
        }

        final Set<Long> results = new HashSet<>();
        final Map<Long, CommandResultCode> codes = new HashMap<>();
        final ResultHandler handler =
                (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                    results.add(idLo);
                    codes.put(idLo, code);
                };

        final Set<Long> submitted = new HashSet<>();
        try {
            final ClientConfig config = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(NODES))
                    .build();
            try (ExcClient client = new ExcClient(config, handler)) {
                awaitLeader(client);

                long uid = 1L;
                for (int i = 0; i < FIRST_BATCH; i++) {
                    submitted.add(submitUser(client, uid++));
                }
                drainUntil(client, () -> results.size() >= FIRST_BATCH);

                final int leader = client.leaderMemberId();
                assertTrue(leader >= 0 && leader < NODES, "a leader must be known before the kill");

                // Kill the leader; the remaining two nodes hold quorum and elect a new one.
                nodes[leader].close();
                nodes[leader] = null;

                for (int i = 0; i < SECOND_BATCH; i++) {
                    submitted.add(submitUser(client, uid++));
                }

                final int expected = FIRST_BATCH + SECOND_BATCH;
                drainUntil(client, () -> results.size() >= expected);

                assertEquals(expected, submitted.size(), "each submission must have a distinct command id");
                assertEquals(expected, results.size(), "no loss: every submission is acknowledged exactly once");
                for (final long id : submitted) {
                    assertEquals(
                            CommandResultCode.SUCCESS,
                            codes.get(id),
                            "no duplication: a re-applied command would not report SUCCESS twice");
                }
            }
        } finally {
            for (final ClusterNode node : nodes) {
                if (node != null) {
                    node.close();
                }
            }
        }
    }

    private static void awaitLeader(final ExcClient client) {
        final long deadline = System.currentTimeMillis() + CONNECT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (client.leaderMemberId() >= 0) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no leader established within timeout");
    }

    private static long submitUser(final ExcClient client, final long uid) {
        final long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                return client.addUser(uid);
            } catch (final BackpressureException e) {
                client.poll();
                Thread.onSpinWait();
            }
        }
        throw new AssertionError("could not submit addUser(" + uid + ") within timeout");
    }

    private static void drainUntil(final ExcClient client, final java.util.function.BooleanSupplier done) {
        final long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (done.getAsBoolean()) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("condition not met within timeout");
    }
}
