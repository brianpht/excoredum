package com.exadbe.write.client.config;

import java.util.concurrent.TimeUnit;

/**
 * Immutable configuration for an {@link com.exadbe.write.client.ExcClient}.
 *
 * <p>When {@code aeronDirectoryName} is {@code null}, the client launches its own
 * embedded media driver, so it survives the shutdown of any individual cluster
 * node.
 */
public final class ClientConfig {

    private final long clientId;
    private final long initialClientSeq;
    private final String ingressEndpoints;
    private final String aeronDirectoryName;
    private final String egressChannel;
    private final long messageTimeoutNs;
    private final long retryBackoffNs;
    private final int maxRetries;
    private final int maxInFlight;
    private final int dedupWindow;
    private final long keepaliveIntervalNs;

    private ClientConfig(final Builder builder) {
        this.clientId = builder.clientId;
        this.initialClientSeq = builder.initialClientSeq;
        this.ingressEndpoints = builder.ingressEndpoints;
        this.aeronDirectoryName = builder.aeronDirectoryName;
        this.egressChannel = builder.egressChannel;
        this.messageTimeoutNs = builder.messageTimeoutNs;
        this.retryBackoffNs = builder.retryBackoffNs;
        this.maxRetries = builder.maxRetries;
        this.maxInFlight = builder.maxInFlight;
        this.dedupWindow = builder.dedupWindow;
        this.keepaliveIntervalNs = builder.keepaliveIntervalNs;
    }

    public static Builder builder(final long clientId, final String ingressEndpoints) {
        return new Builder(clientId, ingressEndpoints);
    }

    public long clientId() {
        return clientId;
    }

    /**
     * The first per-client sequence this incarnation submits. The engine dedups
     * on {@code (clientId, clientSeq)} and those records survive restarts via
     * snapshot, so a restarted process that reuses a {@code clientId} with
     * {@code initialClientSeq} zero receives the OLD cached results instead of
     * fresh applies against a warm cluster. Long-lived deployments must advance
     * this epoch per incarnation (or use a fresh clientId).
     */
    public long initialClientSeq() {
        return initialClientSeq;
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
     * The cluster's per-client dedup window: how many of this client's most
     * recent commands the engine can still recognize as duplicates. A
     * retransmitted command is only safe while its original submission is
     * inside this window; once more than {@code dedupWindow} new commands have
     * been submitted, a resend would be applied a second time. The client
     * therefore expires (never resends) commands that fell out of the window.
     * Must be at most the engine's {@code CoreConfig.dedupWindow} (default
     * 1024).
     */
    public int dedupWindow() {
        return dedupWindow;
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
        private long initialClientSeq;
        private String aeronDirectoryName;
        private String egressChannel = "aeron:udp?endpoint=localhost:0";
        private long messageTimeoutNs = TimeUnit.SECONDS.toNanos(30);
        private long retryBackoffNs = TimeUnit.MILLISECONDS.toNanos(250);
        private int maxRetries;
        private int maxInFlight = 1024;
        private int dedupWindow = 1024;
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

        /**
         * First client sequence for this incarnation; see
         * {@link ClientConfig#initialClientSeq()} for the warm-restart hazard.
         */
        public Builder initialClientSeq(final long value) {
            this.initialClientSeq = value;
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

        /** The cluster's per-client dedup window; see {@link ClientConfig#dedupWindow()}. */
        public Builder dedupWindow(final int value) {
            this.dedupWindow = value;
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
