package com.exadbe.read.config;

import io.aeron.archive.client.AeronArchive;

/**
 * Configuration for a non-voting read replica following one cluster member's
 * Aeron Archive. Endpoints are supplied as raw strings so this module needs no
 * dependency on the launcher's cluster configuration.
 */
public final class ReadReplicaConfig {

    private final String aeronDirectoryName;
    private final String archiveControlChannel;
    private final int archiveControlStreamId;
    private final String localHost;

    private ReadReplicaConfig(
            final String aeronDirectoryName,
            final String archiveControlChannel,
            final int archiveControlStreamId,
            final String localHost) {
        this.aeronDirectoryName = aeronDirectoryName;
        this.archiveControlChannel = archiveControlChannel;
        this.archiveControlStreamId = archiveControlStreamId;
        this.localHost = localHost;
    }

    /**
     * Builds a localhost replica configuration.
     *
     * @param aeronDirectoryName the replica's own (separate) media driver directory
     * @param archiveControlChannel the source member's archive control channel,
     *     e.g. {@code aeron:udp?endpoint=localhost:20104}
     */
    public static ReadReplicaConfig localhost(final String aeronDirectoryName, final String archiveControlChannel) {
        return new ReadReplicaConfig(
                aeronDirectoryName,
                archiveControlChannel,
                AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT,
                "localhost");
    }

    public String aeronDirectoryName() {
        return aeronDirectoryName;
    }

    public String archiveControlChannel() {
        return archiveControlChannel;
    }

    public int archiveControlStreamId() {
        return archiveControlStreamId;
    }

    public String localHost() {
        return localHost;
    }
}
