package com.exadbe.gateway;

import com.exadbe.gateway.config.GatewayConfig;
import com.exadbe.gateway.http.HttpServer;
import com.exadbe.gateway.http.Router;
import com.exadbe.gateway.read.ReadPump;
import com.exadbe.gateway.stream.MarketPump;
import com.exadbe.gateway.stream.StreamBroadcaster;
import com.exadbe.gateway.write.WritePump;
import com.exadbe.read.client.config.ReadClientConfig;
import com.exadbe.write.client.config.ClientConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point for the HTTP gateway: builds the read/write pumps, the router,
 * and the Netty server, then blocks until the process is terminated.
 *
 * <p>Run with {@code ./gradlew :exc-gateway:run --args="--config=..."}.
 */
public final class GatewayLauncher {

    private GatewayLauncher() {}

    public static void main(final String[] args) throws Exception {
        final GatewayConfig config = loadConfig(args);

        final ReadClientConfig readConfig = ReadClientConfig.builder()
                .requestChannel(config.readRequestChannel())
                .responseChannel(config.readResponseChannel())
                .requestStreamId(config.readRequestStreamId())
                .responseStreamId(config.readResponseStreamId())
                .aeronDirectoryName(config.readAeronDir())
                .build();

        final ClientConfig writeConfig = ClientConfig.builder(config.writeClientId(), config.writeIngressEndpoints())
                .initialClientSeq(config.writeInitialClientSeq())
                .egressChannel(config.writeEgressChannel())
                .aeronDirectoryName(config.writeAeronDir())
                // Bound retries so a dead cluster cannot hold a write forever.
                .maxRetries(5)
                .build();

        final ReadPump read = new ReadPump(readConfig);
        final StreamBroadcaster broadcaster = new StreamBroadcaster();
        final WritePump write = new WritePump(writeConfig, broadcaster);
        final HttpServer server =
                new HttpServer(config.httpHost(), config.httpPort(), new Router(read, write, config), broadcaster);
        final MarketPump marketPump =
                new MarketPump(read, broadcaster, config.symbols(), config.marketPumpIntervalMs());
        try {
            server.start();
            if (config.marketPumpIntervalMs() > 0 && !config.symbols().isEmpty()) {
                marketPump.start();
            }
            System.out.println("exc-gateway listening on " + config.httpHost() + ":" + config.httpPort());
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(
                            () -> {
                                marketPump.close();
                                server.stop();
                                write.close();
                                read.close();
                            },
                            "exc-gateway-shutdown"));
            // Block the main thread until the process is terminated.
            new CountDownLatch(1).await();
        } finally {
            server.stop();
            marketPump.close();
            write.close();
            read.close();
        }
    }

    private static GatewayConfig loadConfig(final String[] args) throws Exception {
        final Properties properties = new Properties();
        final Path configFile = findConfig(args);
        if (configFile != null) {
            try (var in = Files.newInputStream(configFile)) {
                properties.load(in);
            }
        }
        return GatewayConfig.fromProperties(properties);
    }

    private static Path findConfig(final String[] args) {
        for (int i = 0; i < args.length; i++) {
            final String arg = args[i];
            if (arg.startsWith("--config=")) {
                return Path.of(arg.substring("--config=".length()));
            }
            if ("--config".equals(arg) && i + 1 < args.length) {
                return Path.of(args[i + 1]);
            }
        }
        return null;
    }
}
