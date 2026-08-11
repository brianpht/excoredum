package com.exadbe.gateway;

import com.exadbe.client.ExcClient;
import com.exadbe.client.config.ClientConfig;
import com.exadbe.config.CoreConfig;
import com.exadbe.gateway.config.GatewayConfig;
import com.exadbe.gateway.core.GatewayAgent;
import com.exadbe.gateway.core.GatewayState;
import com.exadbe.gateway.transport.GatewayRequest;
import com.exadbe.gateway.transport.HttpServer;
import com.exadbe.read.ExcReadReplica;
import com.exadbe.read.config.ReadReplicaConfig;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

/**
 * A REST gateway in front of the cluster. Writes travel through the exc-client
 * SDK (idempotent retry, leader-change resend); reads come from an embedded
 * read replica following a member archive, so they never load the consensus
 * path and are eventually consistent.
 *
 * <p>Runtime layout: Netty event loops serve HTTP I/O only and enqueue pooled
 * request slots; one gateway agent thread owns the client, the replica, and
 * all gateway state. Closing the gateway stops accepting requests, stops the
 * agent, then closes the client and replica.
 */
public final class RestGateway implements AutoCloseable {

    private static final int REGISTRY_CAPACITY = 256;

    private final ExcReadReplica replica;
    private final ExcClient client;
    private final GatewayAgent agent;
    private final Thread agentThread;
    private final HttpServer httpServer;

    private RestGateway(
            final ExcReadReplica replica,
            final ExcClient client,
            final GatewayAgent agent,
            final Thread agentThread,
            final HttpServer httpServer) {
        this.replica = replica;
        this.client = client;
        this.agent = agent;
        this.agentThread = agentThread;
        this.httpServer = httpServer;
    }

    /**
     * Builds and starts a gateway: connects the embedded read replica and the
     * cluster client, starts the agent thread, and binds the HTTP port.
     */
    public static RestGateway launch(final GatewayConfig config) {
        final ManyToOneConcurrentArrayQueue<GatewayRequest> inbound =
                new ManyToOneConcurrentArrayQueue<>(config.requestSlots());
        final ManyToOneConcurrentArrayQueue<GatewayRequest> free =
                new ManyToOneConcurrentArrayQueue<>(config.requestSlots());
        for (int i = 0; i < config.requestSlots(); i++) {
            free.offer(new GatewayRequest());
        }

        final ReadReplicaConfig replicaConfig =
                ReadReplicaConfig.localhost(config.replicaAeronDirectoryName(), config.archiveControlChannel());
        final ExcReadReplica replica = new ExcReadReplica(replicaConfig, CoreConfig.defaults());
        ExcClient client = null;
        HttpServer httpServer = null;
        try {
            final GatewayState state = new GatewayState(REGISTRY_CAPACITY);
            final GatewayAgent agent = new GatewayAgent(config, replica, state, inbound, free);
            final ClientConfig clientConfig = ClientConfig.builder(config.clientId(), config.ingressEndpoints())
                    .aeronDirectoryName(config.clientAeronDirectoryName())
                    .egressChannel(config.egressChannel())
                    .maxInFlight(config.maxInFlight())
                    .build();
            client = new ExcClient(clientConfig, agent);
            agent.bind(client);
            httpServer =
                    new HttpServer(config.port(), inbound, free, config.requestTimeoutNs(), config.maxContentLength());
            httpServer.start();
            final Thread agentThread = new Thread(agent, "gateway-agent");
            agentThread.start();
            return new RestGateway(replica, client, agent, agentThread, httpServer);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            closeQuietly(httpServer, client, replica);
            throw new IllegalStateException("interrupted while starting the gateway", e);
        } catch (final RuntimeException e) {
            closeQuietly(httpServer, client, replica);
            throw e;
        }
    }

    private static void closeQuietly(
            final HttpServer httpServer, final ExcClient client, final ExcReadReplica replica) {
        if (httpServer != null) {
            httpServer.close();
        }
        if (client != null) {
            client.close();
        }
        replica.close();
    }

    /** The actual HTTP port, useful when launched with port 0. */
    public int boundPort() {
        return httpServer.boundPort();
    }

    /** The underlying cluster client, for diagnostics. */
    public ExcClient client() {
        return client;
    }

    /** Blocks until the gateway agent thread exits. */
    public void awaitTermination() throws InterruptedException {
        agentThread.join();
    }

    @Override
    public void close() {
        httpServer.close();
        agent.stop();
        try {
            agentThread.join(TimeUnit.SECONDS.toMillis(5));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        client.close();
        replica.close();
    }
}
