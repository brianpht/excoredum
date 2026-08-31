package io.justrade.write.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.protocol.CommandResultCode;
import io.justrade.write.client.config.ClientConfig;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Session liveness: the cluster closes idle client sessions after its session
 * timeout (10 s default). An idle client must hold its session open with NOP
 * keepalives so a command sent after a long idle gap is still applied instead
 * of being dropped on a dead session.
 */
@Tag("integration")
class ClientKeepaliveIntegrationTest {

    private static final long IDLE_MS = 13_000L;
    private static final long TIMEOUT_MS = 30_000L;

    @Test
    @Timeout(90)
    void idleClientSurvivesClusterSessionTimeout(@TempDir final Path baseDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);

        try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults())) {
            final long[] lastIdLo = {Long.MIN_VALUE};
            final CommandResultCode[] lastCode = {CommandResultCode.NULL_VAL};
            final ResultHandler handler =
                    (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                        lastIdLo[0] = idLo;
                        lastCode[0] = code;
                    };

            final ClientConfig config =
                    ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1)).build();

            try (WriteClient client = new WriteClient(config, handler)) {
                // First command: establish the session.
                await(client, client.addUser(1L), lastIdLo);
                assertEquals(CommandResultCode.SUCCESS, lastCode[0]);

                // Idle past the cluster's 10 s session timeout while polling.
                final long idleDeadline = System.currentTimeMillis() + IDLE_MS;
                while (System.currentTimeMillis() < idleDeadline) {
                    client.poll();
                    Thread.onSpinWait();
                }
                assertTrue(
                        client.keepalives() > 0,
                        "idle client must have sent NOP keepalives; keepalives=" + client.keepalives()
                                + " submitted=" + client.submitted()
                                + " completed=" + client.completed()
                                + " backpressure=" + client.backpressureEvents()
                                + " reconnects=" + client.reconnects()
                                + " pending=" + client.pendingCount()
                                + " lastOffer=" + client.lastOfferResult());

                // A command after the idle gap must still be applied.
                await(client, client.addUser(2L), lastIdLo);
                assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
                assertEquals(0, client.reconnects(), "keepalives should avoid session loss entirely");
            }
        }
    }

    private static void await(final WriteClient client, final long commandIdLo, final long[] lastIdLo) {
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
