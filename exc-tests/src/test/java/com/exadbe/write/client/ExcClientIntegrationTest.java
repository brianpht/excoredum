package com.exadbe.write.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.config.CoreConfig;
import com.exadbe.launcher.ClusterConfig;
import com.exadbe.launcher.ClusterNode;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.protocol.OrderCommandType;
import com.exadbe.write.client.config.ClientConfig;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * P0 exit criteria: a single-node cluster starts, the client submits N commands
 * and receives N results correlated by command id, and the echo service returns
 * a deterministic result for each.
 */
@Tag("integration")
class ExcClientIntegrationTest {

    private static final long TIMEOUT_MS = 15_000L;

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
    void submitsCommandsAndCorrelatesResults() {
        final long[] lastCommandIdLo = {-1L};
        final CommandResultCode[] lastCode = {CommandResultCode.NULL_VAL};
        final long[] lastUid = {Long.MIN_VALUE};
        final AtomicInteger results = new AtomicInteger();

        final ResultHandler handler =
                (idHi, idLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                    lastCommandIdLo[0] = idLo;
                    lastCode[0] = code;
                    if (hasUid) {
                        lastUid[0] = uid;
                    }
                    results.incrementAndGet();
                };

        final ClientConfig config =
                ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1)).build();

        try (ExcClient client = new ExcClient(config, handler)) {
            final int commandCount = 8;
            for (int i = 0; i < commandCount; i++) {
                final long uid = 100L + i;
                final long id = client.submit(OrderCommandType.ADD_USER, uid);
                awaitResult(client, id, lastCommandIdLo);
                assertEquals(CommandResultCode.SUCCESS, lastCode[0]);
                assertEquals(uid, lastUid[0]);
            }

            assertEquals(commandCount, (int) client.completed());
            assertEquals(0, client.pendingCount());
            assertTrue(client.latencyHistogram().getTotalCount() >= commandCount, "latency samples recorded");
        }
    }

    private static void awaitResult(final ExcClient client, final long commandIdLo, final long[] lastCommandIdLo) {
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
