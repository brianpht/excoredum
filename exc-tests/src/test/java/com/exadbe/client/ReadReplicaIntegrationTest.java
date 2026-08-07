package com.exadbe.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.client.config.ClientConfig;
import com.exadbe.config.CoreConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.read.ExcReadReplica;
import com.exadbe.read.config.ReadReplicaConfig;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P5 CQRS read side: a non-voting replica follows the cluster's committed log
 * over its archive and reproduces engine state (users, balances, resting depth)
 * without joining consensus.
 */
@Tag("integration")
class ReadReplicaIntegrationTest {

    private static final long TIMEOUT_MS = 60_000L;
    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 801L;
    private static final long TAKER = 802L;

    @Test
    @Timeout(120)
    void replicaFollowsClusterLog(@TempDir final Path baseDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);

        try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults())) {
            final long[] lastIdLo = {Long.MIN_VALUE};
            final CommandResultCode[] lastCode = {CommandResultCode.NULL_VAL};
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                        lastIdLo[0] = idLo;
                        lastCode[0] = code;
                    };
            final ClientConfig clientConfig =
                    ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1)).build();

            try (ExcClient client = new ExcClient(clientConfig, handler)) {
                await(client, client.addSymbol(SYM, BASE, QUOTE, 1L, 1L), lastIdLo);
                await(client, client.addUser(MAKER), lastIdLo);
                await(client, client.adjustBalance(MAKER, BASE, 1_000L), lastIdLo);
                await(client, client.addUser(TAKER), lastIdLo);
                await(client, client.adjustBalance(TAKER, QUOTE, 1_000_000L), lastIdLo);
                await(client, client.placeGtc(SYM, 1L, true, 100L, 10L, 0L, MAKER), lastIdLo);
                await(client, client.placeGtc(SYM, 2L, false, 105L, 4L, 105L, TAKER), lastIdLo);
                assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
            }

            final ReadReplicaConfig replicaConfig = ReadReplicaConfig.localhost(
                    baseDir.resolve("replica").resolve("driver").toString(), clusterConfig.archiveControlChannel());

            try (ExcReadReplica replica = new ExcReadReplica(replicaConfig, CoreConfig.defaults())) {
                final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
                while (System.currentTimeMillis() < deadline
                        && !(replica.userExists(TAKER) && replica.balance(TAKER, BASE) == 4L)) {
                    replica.poll();
                    Thread.onSpinWait();
                }

                assertTrue(replica.userExists(MAKER), "replica must replicate the maker account");
                assertTrue(replica.userExists(TAKER), "replica must replicate the taker account");
                assertEquals(2, replica.userCount());
                assertEquals(1, replica.symbolCount());
                assertEquals(1, replica.orderCount(), "the maker remainder must rest on the replica book");
                assertEquals(4L, replica.balance(TAKER, BASE), "taker bought 4 base units");
                assertEquals(400L, replica.balance(MAKER, QUOTE), "maker sold 4 base at price 100");
                assertTrue(replica.isHealthy());
                assertTrue(replica.appliedPosition() > 0L);
            }
        }
    }

    private static void await(final ExcClient client, final long commandIdLo, final long[] lastIdLo) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (lastIdLo[0] == commandIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no result for commandIdLo=" + commandIdLo);
    }
}
