package io.justrade.write.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.justrade.config.CoreConfig;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.protocol.CommandResultCode;
import io.justrade.write.client.config.ClientConfig;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P1: account commands over a real single-node cluster. Verifies ADD_USER and
 * BALANCE_ADJUSTMENT result codes end to end via the typed client helpers.
 */
@Tag("integration")
class AccountsIntegrationTest {

    private static final long TIMEOUT_MS = 15_000L;
    private static final int USD = 840;

    private ClusterNode node;

    @BeforeEach
    void startNode(@TempDir final Path baseDir) {
        node = new ClusterNode(ClusterConfig.singleNodeLocalhost(0, baseDir), CoreConfig.defaults());
    }

    @AfterEach
    void stopNode() {
        if (node != null) {
            node.close();
        }
    }

    @Test
    @Timeout(60)
    void accountLifecycleResultCodes() {
        final long[] lastCommandIdLo = {-1L};
        final CommandResultCode[] lastCode = {CommandResultCode.NULL_VAL};

        final ResultHandler handler =
                (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                    lastCommandIdLo[0] = idLo;
                    lastCode[0] = code;
                };

        final ClientConfig config =
                ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1)).build();

        try (WriteClient client = new WriteClient(config, handler)) {
            awaitResult(client, client.addUser(7L), lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);

            awaitResult(client, client.addUser(7L), lastCommandIdLo);
            assertEquals(CommandResultCode.USER_ALREADY_EXISTS, lastCode[0]);

            awaitResult(client, client.adjustBalance(7L, USD, 500L), lastCommandIdLo);
            assertEquals(CommandResultCode.SUCCESS, lastCode[0]);

            awaitResult(client, client.adjustBalance(99L, USD, 500L), lastCommandIdLo);
            assertEquals(CommandResultCode.USER_NOT_FOUND, lastCode[0]);
        }
    }

    private static void awaitResult(final WriteClient client, final long commandIdLo, final long[] lastCommandIdLo) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (lastCommandIdLo[0] == commandIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no result for commandIdLo=" + commandIdLo);
    }
}
