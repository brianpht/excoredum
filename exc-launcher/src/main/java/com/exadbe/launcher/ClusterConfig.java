package com.exadbe.launcher;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Endpoint and directory configuration for a single cluster node.
 *
 * <p>Defaults describe a single-node localhost cluster suitable for local runs
 * and integration tests. Multi-node deployments supply an explicit
 * {@code clusterMembers} string and a matching {@code nodeId}.
 *
 * <p>The {@code clusterMembers} field uses the Aeron member format:
 * {@code id,ingress,consensus,log,catchup,archive} entries separated by
 * {@code |}.
 */
public final class ClusterConfig {

    /** Base ingress port; node {@code n} uses {@code PORT_BASE + n * PORT_STRIDE}. */
    public static final int PORT_BASE = 20100;

    /** Port span reserved per node (ingress, consensus, log, catchup, archive). */
    public static final int PORT_STRIDE = 100;

    private final int nodeId;
    private final String clusterMembers;
    private final String aeronDirectoryName;
    private final File archiveDir;
    private final File clusterDir;
    private final String archiveControlChannel;
    private final String ingressChannel;
    private final String replicationChannel;

    private ClusterConfig(
            final int nodeId,
            final String clusterMembers,
            final String aeronDirectoryName,
            final File archiveDir,
            final File clusterDir,
            final String archiveControlChannel,
            final String ingressChannel,
            final String replicationChannel) {
        this.nodeId = nodeId;
        this.clusterMembers = clusterMembers;
        this.aeronDirectoryName = aeronDirectoryName;
        this.archiveDir = archiveDir;
        this.clusterDir = clusterDir;
        this.archiveControlChannel = archiveControlChannel;
        this.ingressChannel = ingressChannel;
        this.replicationChannel = replicationChannel;
    }

    /** Builds a single-node localhost configuration rooted at {@code baseDir}. */
    public static ClusterConfig singleNodeLocalhost(final int nodeId, final Path baseDir) {
        return build(nodeId, baseDir, "localhost", memberEntry(nodeId, "localhost"));
    }

    /**
     * Builds one configuration per node for an {@code nodeCount}-node localhost
     * cluster. All nodes share the same {@code clusterMembers} string; each node
     * gets its own directory tree under {@code baseDir/node-<id>}.
     *
     * @return an array indexed by node id.
     */
    public static ClusterConfig[] multiNodeLocalhost(final int nodeCount, final Path baseDir) {
        if (nodeCount <= 0) {
            throw new IllegalArgumentException("nodeCount must be positive, was: " + nodeCount);
        }
        final String host = "localhost";
        final StringBuilder members = new StringBuilder();
        for (int i = 0; i < nodeCount; i++) {
            if (i > 0) {
                members.append('|');
            }
            members.append(memberEntry(i, host));
        }
        final String memberString = members.toString();

        final ClusterConfig[] configs = new ClusterConfig[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            configs[i] = build(i, baseDir.resolve("node-" + i), host, memberString);
        }
        return configs;
    }

    /**
     * Loads a node configuration from a {@code .properties} file for deployment.
     *
     * <p>Recognised keys: {@code exc.clusterMembers} (required, member format),
     * {@code exc.baseDir} (directory root, default {@code build/exc-node-<id>}),
     * and {@code exc.host} (this node's host, default {@code localhost}).
     */
    public static ClusterConfig fromProperties(final Path propertiesFile, final int nodeId) {
        final Properties props = new Properties();
        try (InputStream in = Files.newInputStream(propertiesFile)) {
            props.load(in);
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to read cluster config: " + propertiesFile, e);
        }

        final String members = props.getProperty("exc.clusterMembers");
        if (members == null || members.isBlank()) {
            throw new IllegalArgumentException("exc.clusterMembers is required in " + propertiesFile);
        }
        final Path baseDir = Paths.get(props.getProperty("exc.baseDir", "build/exc-node-" + nodeId));
        final String host = props.getProperty("exc.host", "localhost");
        return build(nodeId, baseDir, host, members);
    }

    /**
     * Builds a node configuration from an explicit Aeron {@code clusterMembers}
     * string, this node's {@code host}, and a directory {@code baseDir}.
     */
    public static ClusterConfig fromMembers(
            final int nodeId, final String host, final String members, final Path baseDir) {
        if (members == null || members.isBlank()) {
            throw new IllegalArgumentException("members string is required");
        }
        return build(nodeId, baseDir, host, members);
    }

    /**
     * Builds the client ingress endpoints string ({@code id=host:port,...}) for
     * an {@code nodeCount}-node localhost cluster, as consumed by an Aeron
     * cluster client.
     */
    public static String ingressEndpoints(final int nodeCount) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nodeCount; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(i).append("=localhost:").append(PORT_BASE + (i * PORT_STRIDE));
        }
        return sb.toString();
    }

    /** Builds the {@code id,ingress,consensus,log,catchup,archive} member entry. */
    private static String memberEntry(final int nodeId, final String host) {
        final int portBase = PORT_BASE + (nodeId * PORT_STRIDE);
        return nodeId + ","
                + host + ":" + portBase + ","
                + host + ":" + (portBase + 1) + ","
                + host + ":" + (portBase + 2) + ","
                + host + ":" + (portBase + 3) + ","
                + host + ":" + (portBase + 4);
    }

    /** Assembles a node config given its directory root, host, and members string. */
    private static ClusterConfig build(final int nodeId, final Path baseDir, final String host, final String members) {
        final int archivePort = PORT_BASE + (nodeId * PORT_STRIDE) + 4;
        return new ClusterConfig(
                nodeId,
                members,
                baseDir.resolve("driver").toString(),
                baseDir.resolve("archive").toFile(),
                baseDir.resolve("cluster").toFile(),
                "aeron:udp?endpoint=" + host + ":" + archivePort,
                "aeron:udp?term-length=64k",
                "aeron:udp?endpoint=" + host + ":0");
    }

    public int nodeId() {
        return nodeId;
    }

    public String clusterMembers() {
        return clusterMembers;
    }

    public String aeronDirectoryName() {
        return aeronDirectoryName;
    }

    public File archiveDir() {
        return archiveDir;
    }

    public File clusterDir() {
        return clusterDir;
    }

    public String archiveControlChannel() {
        return archiveControlChannel;
    }

    public String ingressChannel() {
        return ingressChannel;
    }

    public String replicationChannel() {
        return replicationChannel;
    }
}
