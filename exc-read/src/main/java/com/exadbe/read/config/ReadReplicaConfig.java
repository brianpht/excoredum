package com.exadbe.read.config;

import com.exadbe.protocol.QueryStreams;
import io.aeron.archive.client.AeronArchive;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for a non-voting read replica following one or more cluster
 * members' Aeron Archives. Endpoints are supplied as raw strings so this module
 * needs no dependency on the launcher's cluster configuration. The first
 * channel is the primary source; the replica fails over to the remaining
 * channels (in order, round-robin) when the current source dies.
 *
 * <p>Recording positions are cluster-global (every member records the same
 * committed consensus log), so a failover resumes the live-log replay from the
 * position already applied instead of rebuilding the state from scratch. The
 * tuning knobs bound how long a dead source can block the poll thread
 * ({@code archiveMessageTimeoutMs}), how often the replica retries a lost
 * source ({@code failoverBackoffMs}), and how long a source can stay silent
 * before it is declared dead ({@code livenessTimeoutMs}).
 */
public final class ReadReplicaConfig {

    private static final long DEFAULT_FAILOVER_BACKOFF_MS = 250L;
    private static final long DEFAULT_ARCHIVE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(2);
    private static final long DEFAULT_LIVENESS_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    private static final long DEFAULT_SNAPSHOT_POLL_MS = TimeUnit.SECONDS.toMillis(5);
    private static final long DEFAULT_SNAPSHOT_LOAD_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);
    private static final long DEFAULT_CHECKPOINT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);

    private final String aeronDirectoryName;
    private final String[] archiveControlChannels;
    private final int archiveControlStreamId;
    private final String localHost;
    private final String queryRequestChannel;
    private final int queryRequestStreamId;
    private final long failoverBackoffMs;
    private final long archiveMessageTimeoutMs;
    private final long livenessTimeoutMs;
    private final long snapshotPollIntervalMs;
    private final long snapshotLoadTimeoutMs;
    private final Path checkpointFile;
    private final long checkpointIntervalMs;

    private ReadReplicaConfig(final Builder builder) {
        this.aeronDirectoryName = builder.aeronDirectoryName;
        this.archiveControlChannels = builder.archiveControlChannels.clone();
        this.archiveControlStreamId = builder.archiveControlStreamId;
        this.localHost = builder.localHost;
        this.queryRequestChannel = builder.queryRequestChannel;
        this.queryRequestStreamId = builder.queryRequestStreamId;
        this.failoverBackoffMs = builder.failoverBackoffMs;
        this.archiveMessageTimeoutMs = builder.archiveMessageTimeoutMs;
        this.livenessTimeoutMs = builder.livenessTimeoutMs;
        this.snapshotPollIntervalMs = builder.snapshotPollIntervalMs;
        this.snapshotLoadTimeoutMs = builder.snapshotLoadTimeoutMs;
        this.checkpointFile = builder.checkpointFile;
        this.checkpointIntervalMs = builder.checkpointIntervalMs;
    }

    /** Starts a replica configuration with the given media-driver directory. */
    public static Builder builder(final String aeronDirectoryName) {
        return new Builder(aeronDirectoryName);
    }

    /**
     * Builds a localhost replica configuration.
     *
     * @param aeronDirectoryName the replica's own (separate) media driver directory
     * @param archiveControlChannel the primary source member's archive control
     *     channel, e.g. {@code aeron:udp?endpoint=localhost:20104}
     */
    public static ReadReplicaConfig localhost(final String aeronDirectoryName, final String archiveControlChannel) {
        return builder(aeronDirectoryName).channels(archiveControlChannel).build();
    }

    /**
     * Builds a localhost replica configuration with a custom query request
     * channel, for deployments that move the read service off the default port.
     */
    public static ReadReplicaConfig localhost(
            final String aeronDirectoryName,
            final String archiveControlChannel,
            final String queryRequestChannel,
            final int queryRequestStreamId) {
        return builder(aeronDirectoryName)
                .channels(archiveControlChannel)
                .query(queryRequestChannel, queryRequestStreamId)
                .build();
    }

    /**
     * Builds a replica configuration for a deployment where the replica's own
     * address differs from {@code localhost} (e.g. a container on a bridge
     * network). {@code localHost} is the address the replica binds for its
     * archive control responses and log-replay subscription, so the followed
     * member can reach it; the query channel is where the replica answers
     * {@code QueryRequest} frames.
     */
    public static ReadReplicaConfig localhost(
            final String aeronDirectoryName,
            final String archiveControlChannel,
            final String localHost,
            final String queryRequestChannel,
            final int queryRequestStreamId) {
        return builder(aeronDirectoryName)
                .channels(archiveControlChannel)
                .localHost(localHost)
                .query(queryRequestChannel, queryRequestStreamId)
                .build();
    }

    /**
     * Builds a replica configuration with several failover sources, in order of
     * preference. When the current source dies, the replica moves to the next
     * channel (round-robin) and resumes the consensus-log replay from the
     * position already applied - positions are cluster-global, so no rebuild is
     * required unless no source covers the applied position.
     */
    public static ReadReplicaConfig localhost(
            final String aeronDirectoryName,
            final String[] archiveControlChannels,
            final String localHost,
            final String queryRequestChannel,
            final int queryRequestStreamId) {
        return builder(aeronDirectoryName)
                .channels(archiveControlChannels)
                .localHost(localHost)
                .query(queryRequestChannel, queryRequestStreamId)
                .build();
    }

    public String aeronDirectoryName() {
        return aeronDirectoryName;
    }

    /** The primary archive control channel (the first configured source). */
    public String archiveControlChannel() {
        return archiveControlChannels[0];
    }

    /** Every archive control channel, primary first; used for failover. */
    public String[] archiveControlChannels() {
        return archiveControlChannels.clone();
    }

    public int archiveControlStreamId() {
        return archiveControlStreamId;
    }

    public String localHost() {
        return localHost;
    }

    /** The channel the read service subscribes to for {@code QueryRequest} frames. */
    public String queryRequestChannel() {
        return queryRequestChannel;
    }

    /** The stream id the read service subscribes to for {@code QueryRequest} frames. */
    public int queryRequestStreamId() {
        return queryRequestStreamId;
    }

    /** Backoff between failover/connect cycles when no source is reachable. */
    public long failoverBackoffMs() {
        return failoverBackoffMs;
    }

    /** Bounds one archive control operation (connect, list, replay) on the poll thread. */
    public long archiveMessageTimeoutMs() {
        return archiveMessageTimeoutMs;
    }

    /** A source that delivers neither fragments nor successful archive ops within this window is failed over. */
    public long livenessTimeoutMs() {
        return livenessTimeoutMs;
    }

    /** Interval between snapshot polls on the active source (snapshot bootstrap). */
    public long snapshotPollIntervalMs() {
        return snapshotPollIntervalMs;
    }

    /**
     * Maximum time an in-flight snapshot load may take before it is treated as
     * stalled and aborted into a failover. Bounds the wedge a truncated or
     * never-completing snapshot recording would otherwise cause.
     */
    public long snapshotLoadTimeoutMs() {
        return snapshotLoadTimeoutMs;
    }

    /** Local checkpoint file for warm restarts, or {@code null} to disable checkpoints. */
    public Path checkpointFile() {
        return checkpointFile;
    }

    /** Interval between periodic checkpoint writes. */
    public long checkpointIntervalMs() {
        return checkpointIntervalMs;
    }

    /** Fluent builder for a read replica configuration. */
    public static final class Builder {
        private final String aeronDirectoryName;
        private String[] archiveControlChannels;
        private int archiveControlStreamId = AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT;
        private String localHost = "localhost";
        private String queryRequestChannel = QueryStreams.QUERY_REQUEST_CHANNEL;
        private int queryRequestStreamId = QueryStreams.QUERY_REQUEST_STREAM_ID;
        private long failoverBackoffMs = DEFAULT_FAILOVER_BACKOFF_MS;
        private long archiveMessageTimeoutMs = DEFAULT_ARCHIVE_TIMEOUT_MS;
        private long livenessTimeoutMs = DEFAULT_LIVENESS_TIMEOUT_MS;
        private long snapshotPollIntervalMs = DEFAULT_SNAPSHOT_POLL_MS;
        private long snapshotLoadTimeoutMs = DEFAULT_SNAPSHOT_LOAD_TIMEOUT_MS;
        private Path checkpointFile;
        private long checkpointIntervalMs = DEFAULT_CHECKPOINT_INTERVAL_MS;

        private Builder(final String aeronDirectoryName) {
            this.aeronDirectoryName = aeronDirectoryName;
        }

        /** The ordered failover sources, primary first. */
        public Builder channels(final String... channels) {
            if (channels == null || channels.length == 0) {
                throw new IllegalArgumentException("at least one archive control channel is required");
            }
            this.archiveControlChannels = channels.clone();
            return this;
        }

        public Builder localHost(final String value) {
            this.localHost = value;
            return this;
        }

        /** The channel and stream the replica listens on for read-side queries. */
        public Builder query(final String channel, final int streamId) {
            this.queryRequestChannel = channel;
            this.queryRequestStreamId = streamId;
            return this;
        }

        public Builder failoverBackoffMs(final long value) {
            this.failoverBackoffMs = value;
            return this;
        }

        public Builder archiveMessageTimeoutMs(final long value) {
            this.archiveMessageTimeoutMs = value;
            return this;
        }

        public Builder livenessTimeoutMs(final long value) {
            this.livenessTimeoutMs = value;
            return this;
        }

        public Builder snapshotPollIntervalMs(final long value) {
            this.snapshotPollIntervalMs = value;
            return this;
        }

        /** Maximum duration of one snapshot load before it aborts into a failover. */
        public Builder snapshotLoadTimeoutMs(final long value) {
            this.snapshotLoadTimeoutMs = value;
            return this;
        }

        /** Enables periodic + shutdown checkpoint persistence to {@code file}. */
        public Builder checkpointFile(final Path file) {
            this.checkpointFile = file;
            return this;
        }

        public Builder checkpointIntervalMs(final long value) {
            this.checkpointIntervalMs = value;
            return this;
        }

        public ReadReplicaConfig build() {
            if (archiveControlChannels == null || archiveControlChannels.length == 0) {
                throw new IllegalArgumentException("at least one archive control channel is required");
            }
            return new ReadReplicaConfig(this);
        }
    }
}
