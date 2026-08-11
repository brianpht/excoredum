package com.exadbe.gateway.config;

import java.util.concurrent.TimeUnit;

/**
 * Immutable configuration for a REST gateway. Writes go to the cluster through
 * the exc-client SDK; reads come from an embedded read replica following the
 * member archive named by {@code archiveControlChannel}.
 */
public final class GatewayConfig {

    private final int port;
    private final long clientId;
    private final int gatewayId;
    private final String ingressEndpoints;
    private final String archiveControlChannel;
    private final String clientAeronDirectoryName;
    private final String replicaAeronDirectoryName;
    private final String egressChannel;
    private final long requestTimeoutNs;
    private final int maxInFlight;
    private final int maxContentLength;
    private final int requestSlots;

    private GatewayConfig(final Builder builder) {
        this.port = builder.port;
        this.clientId = builder.clientId;
        this.gatewayId = builder.gatewayId;
        this.ingressEndpoints = builder.ingressEndpoints;
        this.archiveControlChannel = builder.archiveControlChannel;
        this.clientAeronDirectoryName = builder.clientAeronDirectoryName;
        this.replicaAeronDirectoryName = builder.replicaAeronDirectoryName;
        this.egressChannel = builder.egressChannel;
        this.requestTimeoutNs = builder.requestTimeoutNs;
        this.maxInFlight = builder.maxInFlight;
        this.maxContentLength = builder.maxContentLength;
        this.requestSlots = builder.requestSlots;
    }

    public static Builder builder(
            final long clientId,
            final String ingressEndpoints,
            final String archiveControlChannel,
            final String replicaAeronDirectoryName) {
        return new Builder(clientId, ingressEndpoints, archiveControlChannel, replicaAeronDirectoryName);
    }

    public int port() {
        return port;
    }

    /** Cluster client identity used on every submitted command envelope. */
    public long clientId() {
        return clientId;
    }

    /** Instance id mixed into minted order ids so gateway instances never collide. */
    public int gatewayId() {
        return gatewayId;
    }

    public String ingressEndpoints() {
        return ingressEndpoints;
    }

    /** The member archive control channel the embedded replica follows. */
    public String archiveControlChannel() {
        return archiveControlChannel;
    }

    /** Media driver directory for the cluster client; null launches an embedded driver. */
    public String clientAeronDirectoryName() {
        return clientAeronDirectoryName;
    }

    /** Media driver directory for the embedded read replica (its own driver). */
    public String replicaAeronDirectoryName() {
        return replicaAeronDirectoryName;
    }

    public String egressChannel() {
        return egressChannel;
    }

    /** Gateway-side deadline per request; a miss answers 504, the command keeps retrying. */
    public long requestTimeoutNs() {
        return requestTimeoutNs;
    }

    public int maxInFlight() {
        return maxInFlight;
    }

    public int maxContentLength() {
        return maxContentLength;
    }

    /** Pooled request slot count; a power of two backing both lock-free queues. */
    public int requestSlots() {
        return requestSlots;
    }

    /** Fluent builder with defaults for local and production use. */
    public static final class Builder {
        private final long clientId;
        private final String ingressEndpoints;
        private final String archiveControlChannel;
        private final String replicaAeronDirectoryName;
        private int port = 8080;
        private int gatewayId = 1;
        private String clientAeronDirectoryName;
        private String egressChannel = "aeron:udp?endpoint=localhost:0";
        private long requestTimeoutNs = TimeUnit.SECONDS.toNanos(10);
        private int maxInFlight = 1024;
        private int maxContentLength = 64 * 1024;
        private int requestSlots = 1024;

        private Builder(
                final long clientId,
                final String ingressEndpoints,
                final String archiveControlChannel,
                final String replicaAeronDirectoryName) {
            this.clientId = clientId;
            this.ingressEndpoints = ingressEndpoints;
            this.archiveControlChannel = archiveControlChannel;
            this.replicaAeronDirectoryName = replicaAeronDirectoryName;
        }

        public Builder port(final int value) {
            this.port = value;
            return this;
        }

        public Builder gatewayId(final int value) {
            this.gatewayId = value;
            return this;
        }

        /** Attach the cluster client to an existing media driver rather than an embedded one. */
        public Builder clientAeronDirectoryName(final String value) {
            this.clientAeronDirectoryName = value;
            return this;
        }

        public Builder egressChannel(final String value) {
            this.egressChannel = value;
            return this;
        }

        public Builder requestTimeoutNs(final long value) {
            this.requestTimeoutNs = value;
            return this;
        }

        public Builder maxInFlight(final int value) {
            this.maxInFlight = value;
            return this;
        }

        public Builder maxContentLength(final int value) {
            this.maxContentLength = value;
            return this;
        }

        public Builder requestSlots(final int value) {
            this.requestSlots = value;
            return this;
        }

        public GatewayConfig build() {
            if (port < 0 || port > 65535) {
                throw new IllegalArgumentException("port out of range: " + port);
            }
            if (gatewayId <= 0 || gatewayId > 0x7FFF) {
                throw new IllegalArgumentException("gatewayId must fit 15 bits: " + gatewayId);
            }
            if (requestSlots <= 0 || (requestSlots & (requestSlots - 1)) != 0) {
                throw new IllegalArgumentException("requestSlots must be a power of two: " + requestSlots);
            }
            if (requestTimeoutNs <= 0L) {
                throw new IllegalArgumentException("requestTimeoutNs must be positive");
            }
            if (maxInFlight <= 0) {
                throw new IllegalArgumentException("maxInFlight must be positive");
            }
            if (maxContentLength <= 0) {
                throw new IllegalArgumentException("maxContentLength must be positive");
            }
            return new GatewayConfig(this);
        }
    }
}
