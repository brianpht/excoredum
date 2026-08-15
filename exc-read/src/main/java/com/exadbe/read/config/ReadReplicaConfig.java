package com.exadbe.read.config;

import com.exadbe.protocol.QueryStreams;
import io.aeron.archive.client.AeronArchive;

/**
 * Configuration for a non-voting read replica following one or more cluster
 * members' Aeron Archives. Endpoints are supplied as raw strings so this module
 * needs no dependency on the launcher's cluster configuration. The first
 * channel is the primary source; the replica fails over to the remaining
 * channels (in order, round-robin) when the current source dies.
 */
public final class ReadReplicaConfig {

    private final String aeronDirectoryName;
    private final String[] archiveControlChannels;
    private final int archiveControlStreamId;
    private final String localHost;
    private final String queryRequestChannel;
    private final int queryRequestStreamId;

    private ReadReplicaConfig(
            final String aeronDirectoryName,
            final String[] archiveControlChannels,
            final int archiveControlStreamId,
            final String localHost,
            final String queryRequestChannel,
            final int queryRequestStreamId) {
        this.aeronDirectoryName = aeronDirectoryName;
        this.archiveControlChannels = archiveControlChannels.clone();
        this.archiveControlStreamId = archiveControlStreamId;
        this.localHost = localHost;
        this.queryRequestChannel = queryRequestChannel;
        this.queryRequestStreamId = queryRequestStreamId;
    }

    /**
     * Builds a localhost replica configuration.
     *
     * @param aeronDirectoryName the replica's own (separate) media driver directory
     * @param archiveControlChannel the primary source member's archive control
     *     channel, e.g. {@code aeron:udp?endpoint=localhost:20104}
     */
    public static ReadReplicaConfig localhost(final String aeronDirectoryName, final String archiveControlChannel) {
        return localhost(
                aeronDirectoryName,
                archiveControlChannel,
                "localhost",
                QueryStreams.QUERY_REQUEST_CHANNEL,
                QueryStreams.QUERY_REQUEST_STREAM_ID);
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
        return localhost(
                aeronDirectoryName, archiveControlChannel, "localhost", queryRequestChannel, queryRequestStreamId);
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
        return localhost(
                aeronDirectoryName,
                new String[] {archiveControlChannel},
                localHost,
                queryRequestChannel,
                queryRequestStreamId);
    }

    /**
     * Builds a replica configuration with several failover sources, in order of
     * preference. When the current source dies, the replica moves to the next
     * channel (round-robin) and rebuilds its state by replaying that member's
     * consensus-log recording from the start.
     */
    public static ReadReplicaConfig localhost(
            final String aeronDirectoryName,
            final String[] archiveControlChannels,
            final String localHost,
            final String queryRequestChannel,
            final int queryRequestStreamId) {
        if (archiveControlChannels == null || archiveControlChannels.length == 0) {
            throw new IllegalArgumentException("at least one archive control channel is required");
        }
        return new ReadReplicaConfig(
                aeronDirectoryName,
                archiveControlChannels,
                AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT,
                localHost,
                queryRequestChannel,
                queryRequestStreamId);
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
}
