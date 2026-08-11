package com.exadbe.client.config;

import java.util.concurrent.TimeUnit;

/**
 * Immutable configuration for an {@link com.exadbe.client.ExcClient}.
 *
 * <p>When {@code aeronDirectoryName} is {@code null}, the client launches its own
 * embedded media driver, so it survives the shutdown of any individual cluster
 * node.
 */
public final class ClientConfig {

    private final long clientId;
    private final String ingressEndpoints;
    private final String aeronDirectoryName;
    private final String egressChannel;
    private final long messageTimeoutNs;
    private final long retryBackoffNs;
    private final int maxRetries;
    private final int maxInFlight;
    private final long keepaliveIntervalNs;

    private ClientConfig(final Builder builder) {
        this.clientId = builder.clientId;
        this.ingressEndpoints = builder.ingressEndpoints;
        this.aeronDirectoryName = builder.aeronDirectoryName;
        this.egressChannel = builder.egressChannel;
        this.messageTimeoutNs = builder.messageTimeoutNs;
        this.retryBackoffNs = builder.retryBackoffNs;
        this.maxRetries = builder.maxRetries;
        this.maxInFlight = builder.maxInFlight;
        this.keepaliveIntervalNs = builder.keepaliveIntervalNs;
    }

    public static Builder builder(final long clientId, final String ingressEndpoints) {
        return new Builder(clientId, ingressEndpoints);
    }

    public long clientId() {
        return clientId;
    }

    public String ingressEndpoints() {
        return ingressEndpoints;
    }

    public String aeronDirectoryName() {
        return aeronDirectoryName;
    }

    public String egressChannel() {
        return egressChannel;
    }

    public long messageTimeoutNs() {
        return messageTimeoutNs;
    }

    public long retryBackoffNs() {
        return retryBackoffNs;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public int maxInFlight() {
        return maxInFlight;
    }

    /**
     * Idle period after which the client submits a NOP keepalive to hold its
     * cluster session open; the cluster closes idle sessions after its session
     * timeout (10 s by default). Zero disables keepalives.
     */
    public long keepaliveIntervalNs() {
        return keepaliveIntervalNs;
    }

    /** Fluent builder with sensible defaults for local and production use. */
    public static final class Builder {
        private final long clientId;
        private final String ingressEndpoints;
        private String aeronDirectoryName;
        private String egressChannel = "aeron:udp?endpoint=localhost:0";
        private long messageTimeoutNs = TimeUnit.SECONDS.toNanos(30);
        private long retryBackoffNs = TimeUnit.MILLISECONDS.toNanos(250);
        private int maxRetries;
        private int maxInFlight = 1024;
        private long keepaliveIntervalNs = TimeUnit.SECONDS.toNanos(2);

        private Builder(final long clientId, final String ingressEndpoints) {
            this.clientId = clientId;
            this.ingressEndpoints = ingressEndpoints;
        }

        /** Attach to an existing media driver rather than launching an embedded one. */
        public Builder aeronDirectoryName(final String value) {
            this.aeronDirectoryName = value;
            return this;
        }

        /** Egress (result) channel the client binds and advertises to the cluster. */
        public Builder egressChannel(final String value) {
            this.egressChannel = value;
            return this;
        }

        public Builder messageTimeoutNs(final long value) {
            this.messageTimeoutNs = value;
            return this;
        }

        public Builder retryBackoffNs(final long value) {
            this.retryBackoffNs = value;
            return this;
        }

        /** Maximum resend attempts per command; {@code 0} means retry indefinitely. */
        public Builder maxRetries(final int value) {
            this.maxRetries = value;
            return this;
        }

        public Builder maxInFlight(final int value) {
            this.maxInFlight = value;
            return this;
        }

        /** Idle interval between NOP keepalives; {@code 0} disables keepalives. */
        public Builder keepaliveIntervalNs(final long value) {
            this.keepaliveIntervalNs = value;
            return this;
        }

        public ClientConfig build() {
            return new ClientConfig(this);
        }
    }
}
