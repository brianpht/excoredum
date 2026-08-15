package com.exadbe.read;

import com.exadbe.config.CoreConfig;
import com.exadbe.core.CommandOutcome;
import com.exadbe.engine.MatchingEngine;
import com.exadbe.engine.orderbook.L2View;
import com.exadbe.engine.orderbook.OrderBookNaive;
import com.exadbe.read.config.ReadReplicaConfig;
import com.exadbe.read.order.MarketTrade;
import com.exadbe.read.order.OrderLedger;
import com.exadbe.read.order.OrderRecord;
import com.exadbe.read.report.ReportGenerator;
import com.exadbe.read.report.SingleUserReport;
import com.exadbe.read.report.TotalCurrencyBalance;
import com.exadbe.telemetry.CoreMetrics;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.util.List;

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
 *
 * <p>The replica is configured with an ordered list of member archives (see
 * {@link ReadReplicaConfig}). When the current source dies it fails over to the
 * next member: recording positions are member-specific, so the replicated state
 * is rebuilt by clearing the engine and ledger and replaying the new member's
 * recording from the start - the read model is eventually consistent, so a
 * brief catch-up window after failover is part of its contract.
 */
public final class ExcReadReplica implements AutoCloseable {

    private static final int FRAGMENT_LIMIT = 64;
    private static final long RECONNECT_BACKOFF_MS = 100L;

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final String[] archiveControlChannels;
    private final int archiveControlStreamId;
    private final MatchingEngine engine;
    private final CommandOutcome outcome;
    private final OrderLedger ledger = new OrderLedger();
    private final ReplicationHealth health = new ReplicationHealth();
    private final String localHost;
    private final ReportGenerator reports;

    private ReplicaCommandListener commandListener = ReplicaCommandListener.NONE;
    private LiveLogSubscriber liveLog;
    private AeronArchive archive;
    private int currentSource;
    private long appliedPosition;
    private long nextConnectMs;

    public ExcReadReplica(final ReadReplicaConfig config, final CoreConfig coreConfig) {
        this.engine = new MatchingEngine(coreConfig, new CoreMetrics());
        this.outcome = new CommandOutcome(coreConfig.eventBufferCapacity());
        this.localHost = config.localHost();
        this.reports = new ReportGenerator(engine);
        this.archiveControlChannels = config.archiveControlChannels();
        this.archiveControlStreamId = config.archiveControlStreamId();

        MediaDriver driver = null;
        Aeron aeronClient = null;
        try {
            driver = MediaDriver.launch(new MediaDriver.Context()
                    .aeronDirectoryName(config.aeronDirectoryName())
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true));
            aeronClient = Aeron.connect(new Aeron.Context().aeronDirectoryName(config.aeronDirectoryName()));
        } catch (final RuntimeException e) {
            closeQuietly(aeronClient, driver);
            throw e;
        }
        this.mediaDriver = driver;
        this.aeron = aeronClient;
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
        for (int attempt = 0; attempt < archiveControlChannels.length; attempt++) {
            final int idx = (currentSource + attempt) % archiveControlChannels.length;
            if (idx != currentSource) {
                // Failing over to another member: recording positions are
                // member-specific, so rebuild the replicated state from scratch
                // and replay that member's recording from the start.
                resetReplication();
                currentSource = idx;
            }
            closeArchive();
            if (!connectArchive(idx)) {
                continue;
            }
            final LiveLogSubscriber subscriber = new LiveLogSubscriber(
                    archive, engine, outcome, ledger, commandListener, appliedPosition, localHost);
            if (subscriber.connect()) {
                liveLog = subscriber;
                return;
            }
            subscriber.close();
        }
        closeArchive();
        nextConnectMs = System.currentTimeMillis() + RECONNECT_BACKOFF_MS;
    }

    private boolean connectArchive(final int idx) {
        try {
            archive = AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .controlRequestChannel(archiveControlChannels[idx])
                    .controlRequestStreamId(archiveControlStreamId)
                    .controlResponseChannel("aeron:udp?endpoint=" + localHost + ":0"));
            return true;
        } catch (final RuntimeException e) {
            archive = null;
            return false;
        }
    }

    private void closeArchive() {
        if (archive != null) {
            archive.close();
            archive = null;
        }
    }

    private void resetReplication() {
        engine.clearState();
        ledger.clear();
        outcome.reset(0L, 0L);
        appliedPosition = 0L;
    }

    /**
     * Registers a callback fired for every command applied from the followed
     * log, with the leader-assigned timestamp and the command outcome. Must be
     * set before the replica starts polling; the gateway agent uses it to push
     * real-time market events over WebSocket.
     */
    public void setCommandListener(final ReplicaCommandListener listener) {
        this.commandListener = listener == null ? ReplicaCommandListener.NONE : listener;
    }

    /**
     * Whether the replica has replayed past the recording position observed at
     * connect and is following live, so command callbacks carry recent events
     * rather than history. False while disconnected or during initial replay.
     */
    public boolean isCaughtUp() {
        return liveLog != null && liveLog.isCaughtUp();
    }

    /** The cluster-global log position consumed so far. */
    public long appliedPosition() {
        return appliedPosition;
    }

    /** Index of the member archive currently followed (0 = the primary source). */
    public int currentSource() {
        return currentSource;
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

    /**
     * Fills {@code view} with an L2 snapshot of {@code symbolId} from the
     * replicated book, bounded by {@code view.maxLevels()}. Returns false when
     * the symbol is not (yet) replicated, leaving the view untouched.
     */
    public boolean orderBook(final int symbolId, final L2View view) {
        final OrderBookNaive book = engine.book(symbolId);
        if (book == null) {
            return false;
        }
        book.fillL2(view);
        return true;
    }

    /** Balances and resting orders for {@code uid}, from the replicated state. */
    public SingleUserReport singleUserReport(final long uid) {
        return reports.singleUser(uid);
    }

    /** Every tracked order of {@code uid} in placement order, from the replicated log. */
    public List<OrderRecord> orderHistory(final long uid) {
        return ledger.orderHistory(uid);
    }

    /** The still-resting orders of {@code uid}, from the replicated log. */
    public List<OrderRecord> activeOrders(final long uid) {
        return ledger.activeOrders(uid);
    }

    /** The tracked record for {@code orderId}, or {@code null} when unknown. */
    public OrderRecord order(final long orderId) {
        return ledger.order(orderId);
    }

    /** The most recent {@code limit} trades involving {@code uid} as maker or taker. */
    public List<MarketTrade> userTrades(final long uid, final int limit) {
        return ledger.userTrades(uid, limit);
    }

    /** The most recent {@code limit} trades of {@code symbolId}. */
    public List<MarketTrade> marketTrades(final int symbolId, final int limit) {
        return ledger.marketTrades(symbolId, limit);
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
        closeArchive();
        closeQuietly(aeron, mediaDriver);
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
