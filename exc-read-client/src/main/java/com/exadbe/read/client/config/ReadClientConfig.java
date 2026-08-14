package com.exadbe.read.client.config;

import com.exadbe.protocol.QueryStreams;
import java.util.concurrent.TimeUnit;

/**
 * Immutable configuration for a {@link com.exadbe.read.client.ReadClient}.
 *
 * <p>When {@code aeronDirectoryName} is {@code null} the client launches its
 * own embedded media driver, so it can reach a read replica on another process
 * over UDP localhost. The request channel defaults to the read replica's
 * {@link QueryStreams#QUERY_REQUEST_CHANNEL} so an out-of-the-box client talks
 * to an out-of-the-box read service.
 */
public final class ReadClientConfig {

    private final String requestChannel;
    private final int requestStreamId;
    private final int responseStreamId;
    private final String aeronDirectoryName;
    private final long messageTimeoutNs;
    private final long retryBackoffNs;
    private final int maxRetries;
    private final int maxInFlight;

    private ReadClientConfig(final Builder builder) {
        this.requestChannel = builder.requestChannel;
        this.requestStreamId = builder.requestStreamId;
        this.responseStreamId = builder.responseStreamId;
        this.aeronDirectoryName = builder.aeronDirectoryName;
        this.messageTimeoutNs = builder.messageTimeoutNs;
        this.retryBackoffNs = builder.retryBackoffNs;
        this.maxRetries = builder.maxRetries;
        this.maxInFlight = builder.maxInFlight;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String requestChannel() {
        return requestChannel;
    }

    public int requestStreamId() {
        return requestStreamId;
    }

    /** The stream id the client listens on for {@code QueryResponse} frames. */
    public int responseStreamId() {
        return responseStreamId;
    }

    public String aeronDirectoryName() {
        return aeronDirectoryName;
    }

    public long messageTimeoutNs() {
        return messageTimeoutNs;
    }

    public long retryBackoffNs() {
        return retryBackoffNs;
    }

    /** Maximum retries per query; {@code 0} means retry until the overall call budget is spent. */
    public int maxRetries() {
        return maxRetries;
    }

    /** Maximum in-flight queries before {@code submit} signals backpressure. */
    public int maxInFlight() {
        return maxInFlight;
    }

    /** Fluent builder with sensible defaults for local and production use. */
    public static final class Builder {
        private String requestChannel = QueryStreams.QUERY_REQUEST_CHANNEL;
        private int requestStreamId = QueryStreams.QUERY_REQUEST_STREAM_ID;
        private int responseStreamId = QueryStreams.QUERY_RESPONSE_STREAM_ID;
        private String aeronDirectoryName;
        private long messageTimeoutNs = TimeUnit.SECONDS.toNanos(5);
        private long retryBackoffNs = TimeUnit.MILLISECONDS.toNanos(250);
        private int maxRetries = 5;
        private int maxInFlight = 1024;

        /** The read service's query request channel, e.g. {@code aeron:udp?endpoint=host:44000}. */
        public Builder requestChannel(final String value) {
            this.requestChannel = value;
            return this;
        }

        public Builder requestStreamId(final int value) {
            this.requestStreamId = value;
            return this;
        }

        public Builder responseStreamId(final int value) {
            this.responseStreamId = value;
            return this;
        }

        /** Attach to an existing media driver rather than launching an embedded one. */
        public Builder aeronDirectoryName(final String value) {
            this.aeronDirectoryName = value;
            return this;
        }

        /** Per-attempt deadline for a query response. */
        public Builder messageTimeoutNs(final long value) {
            this.messageTimeoutNs = value;
            return this;
        }

        /** Pause between retry attempts. */
        public Builder retryBackoffNs(final long value) {
            this.retryBackoffNs = value;
            return this;
        }

        public Builder maxRetries(final int value) {
            this.maxRetries = value;
            return this;
        }

        public Builder maxInFlight(final int value) {
            this.maxInFlight = value;
            return this;
        }

        public ReadClientConfig build() {
            return new ReadClientConfig(this);
        }
    }
}
