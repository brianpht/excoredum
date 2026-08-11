package com.exadbe.gateway;

import com.exadbe.gateway.config.GatewayConfig;
import java.nio.file.Files;

/**
 * Launches a standalone REST gateway that bridges HTTP/JSON to a running
 * cluster: writes through the client SDK, reads from an embedded read replica
 * following the named member archive.
 *
 * <pre>{@code
 * ./gradlew :exc-gateway-rest:run \
 *     --args="--port=8080 --ingress=0=localhost:20100 --archive=aeron:udp?endpoint=localhost:20104"
 * }</pre>
 */
public final class RestGatewayLauncher {

    // Defaults describe a single-node localhost cluster (see exc-launcher).
    private static final int DEFAULT_PORT = 8080;
    private static final String DEFAULT_INGRESS = "0=localhost:20100";
    private static final String DEFAULT_ARCHIVE = "aeron:udp?endpoint=localhost:20104";

    private RestGatewayLauncher() {}

    public static void main(final String[] args) throws Exception {
        int port = DEFAULT_PORT;
        long clientId = 0L;
        boolean clientIdSet = false;
        int gatewayId = 1;
        String ingress = DEFAULT_INGRESS;
        String archive = DEFAULT_ARCHIVE;
        String clientAeronDir = null;
        for (final String arg : args) {
            final int eq = arg.indexOf('=');
            if (eq >= 0 && arg.startsWith("--port=")) {
                port = Integer.parseInt(arg.substring(eq + 1));
            } else if (eq >= 0 && arg.startsWith("--ingress=")) {
                ingress = arg.substring(eq + 1);
            } else if (eq >= 0 && arg.startsWith("--archive=")) {
                archive = arg.substring(eq + 1);
            } else if (eq >= 0 && arg.startsWith("--clientId=")) {
                clientId = Long.parseLong(arg.substring(eq + 1));
                clientIdSet = true;
            } else if (eq >= 0 && arg.startsWith("--gatewayId=")) {
                gatewayId = Integer.parseInt(arg.substring(eq + 1));
            } else if (eq >= 0 && arg.startsWith("--aeronDir=")) {
                clientAeronDir = arg.substring(eq + 1);
            } else {
                throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }

        // A stable clientId plus a clientSeq that restarts at zero would replay
        // the cluster's dedup table after a gateway restart (cached results,
        // commands silently not applied). Default to a per-process unique id so
        // restarts start a fresh dedup identity; pass --clientId to pin one.
        if (!clientIdSet) {
            clientId = uniqueClientId();
        }

        final String replicaDir =
                Files.createTempDirectory("exc-gw-replica-").resolve("driver").toString();
        final GatewayConfig config = GatewayConfig.builder(clientId, ingress, archive, replicaDir)
                .port(port)
                .gatewayId(gatewayId)
                .clientAeronDirectoryName(clientAeronDir)
                .build();

        try (RestGateway gateway = RestGateway.launch(config)) {
            System.out.println("exc-gateway-rest listening on http://localhost:" + gateway.boundPort() + " (ingress="
                    + ingress + ", archive=" + archive + ", clientId=" + clientId + ") - Ctrl-C to stop");
            gateway.awaitTermination();
        }
    }

    /** A positive id unique to this process start: epoch millis + sub-ms counter. */
    private static long uniqueClientId() {
        final long nanos = System.nanoTime();
        return System.currentTimeMillis() * 1_000_000L + Math.floorMod(nanos, 1_000_000L);
    }
}
