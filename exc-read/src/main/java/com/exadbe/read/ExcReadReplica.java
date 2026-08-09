package com.exadbe.read;

import com.exadbe.config.CoreConfig;
import com.exadbe.core.CommandOutcome;
import com.exadbe.engine.MatchingEngine;
import com.exadbe.read.config.ReadReplicaConfig;
import com.exadbe.read.report.ReportGenerator;
import com.exadbe.read.report.SingleUserReport;
import com.exadbe.read.report.TotalCurrencyBalance;
import com.exadbe.telemetry.CoreMetrics;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

/**
 * A non-voting read replica that follows a cluster member's committed consensus
 * log over its Aeron Archive and applies it to a private {@link MatchingEngine},
 * serving eventually-consistent reads. It never joins Raft and never affects
 * quorum.
 *
 * <p>Poll-driven, single-threaded: the caller drives {@link #poll()} and issues
 * reads from the same thread, so the engine's non-thread-safe stores are only
 * ever touched by one thread and readers always see a consistent state. The
 * replica follows the log from the start; engine dedup makes any re-delivered
 * prefix idempotent.
 */
public final class ExcReadReplica implements AutoCloseable {

    private static final int FRAGMENT_LIMIT = 64;
    private static final long RECONNECT_BACKOFF_MS = 100L;

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final MatchingEngine engine;
    private final CommandOutcome outcome;
    private final ReplicationHealth health = new ReplicationHealth();
    private final String localHost;
    private final ReportGenerator reports;

    private LiveLogSubscriber liveLog;
    private long appliedPosition;
    private long nextConnectMs;

    public ExcReadReplica(final ReadReplicaConfig config, final CoreConfig coreConfig) {
        this.engine = new MatchingEngine(coreConfig, new CoreMetrics());
        this.outcome = new CommandOutcome(coreConfig.eventBufferCapacity());
        this.localHost = config.localHost();
        this.reports = new ReportGenerator(engine);

        MediaDriver driver = null;
        Aeron aeronClient = null;
        AeronArchive archiveClient = null;
        try {
            driver = MediaDriver.launch(new MediaDriver.Context()
                    .aeronDirectoryName(config.aeronDirectoryName())
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true));
            aeronClient = Aeron.connect(new Aeron.Context().aeronDirectoryName(config.aeronDirectoryName()));
            archiveClient = AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeronClient)
                    .controlRequestChannel(config.archiveControlChannel())
                    .controlRequestStreamId(config.archiveControlStreamId())
                    .controlResponseChannel("aeron:udp?endpoint=" + config.localHost() + ":0"));
        } catch (final RuntimeException e) {
            closeQuietly(archiveClient, aeronClient, driver);
            throw e;
        }
        this.mediaDriver = driver;
        this.aeron = aeronClient;
        this.archive = archiveClient;
    }

    /** Advances replication by polling the live log; call repeatedly from one thread. */
    public int poll() {
        ensureLiveLog();
        if (liveLog == null) {
            health.markStale();
            return 0;
        }
        final int fragments = liveLog.poll(FRAGMENT_LIMIT);
        if (fragments > 0) {
            final long position = liveLog.lastPosition();
            if (position > appliedPosition) {
                appliedPosition = position;
            }
        }
        health.markHealthy(appliedPosition);
        if (liveLog.isReplayEnded()) {
            liveLog.close();
            liveLog = null;
            nextConnectMs = System.currentTimeMillis() + RECONNECT_BACKOFF_MS;
        }
        return fragments;
    }

    private void ensureLiveLog() {
        if (liveLog != null || System.currentTimeMillis() < nextConnectMs) {
            return;
        }
        final LiveLogSubscriber subscriber =
                new LiveLogSubscriber(archive, engine, outcome, appliedPosition, localHost);
        if (subscriber.connect()) {
            liveLog = subscriber;
        } else {
            subscriber.close();
            nextConnectMs = System.currentTimeMillis() + RECONNECT_BACKOFF_MS;
        }
    }

    /** The cluster-global log position consumed so far. */
    public long appliedPosition() {
        return appliedPosition;
    }

    public boolean isHealthy() {
        return health.isHealthy();
    }

    public boolean userExists(final long uid) {
        return engine.userExists(uid);
    }

    public long balance(final long uid, final int currency) {
        return engine.balance(uid, currency);
    }

    public int userCount() {
        return engine.userCount();
    }

    public int symbolCount() {
        return engine.symbolCount();
    }

    public int orderCount() {
        return engine.orderCount();
    }

    /** Balances and resting orders for {@code uid}, from the replicated state. */
    public SingleUserReport singleUserReport(final long uid) {
        return reports.singleUser(uid);
    }

    /** Per-currency total of all balances plus funds reserved by resting orders. */
    public TotalCurrencyBalance totalCurrencyBalance() {
        return reports.totalCurrencyBalance();
    }

    /** Deterministic fingerprint of the replicated state, matching a snapshot checksum. */
    public long stateHash() {
        return reports.stateHash();
    }

    @Override
    public void close() {
        if (liveLog != null) {
            liveLog.close();
            liveLog = null;
        }
        closeQuietly(archive, aeron, mediaDriver);
    }

    private static void closeQuietly(final AutoCloseable... resources) {
        for (final AutoCloseable resource : resources) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (final Exception ignored) {
                    // Best-effort teardown.
                }
            }
        }
    }
}
