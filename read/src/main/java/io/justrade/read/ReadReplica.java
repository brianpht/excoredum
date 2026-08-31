package io.justrade.read;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.justrade.config.CoreConfig;
import io.justrade.core.CommandOutcome;
import io.justrade.engine.MatchingEngine;
import io.justrade.engine.orderbook.L2View;
import io.justrade.engine.orderbook.OrderBookNaive;
import io.justrade.read.config.ReadReplicaConfig;
import io.justrade.read.order.MarketTrade;
import io.justrade.read.order.OrderLedger;
import io.justrade.read.order.OrderRecord;
import io.justrade.read.report.ReportGenerator;
import io.justrade.read.report.SingleUserReport;
import io.justrade.read.report.TotalCurrencyBalance;
import io.justrade.telemetry.CoreMetrics;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A non-voting read replica that follows a cluster member's committed consensus
 * log over its Aeron Archive and applies it to a private {@link MatchingEngine},
 * serving eventually-consistent reads. It never joins Raft and never affects
 * quorum.
 *
 * <p>Poll-driven, single-threaded: the caller drives {@link #poll()} and issues
 * reads from the same thread, so the engine's non-thread-safe stores are only
 * ever touched by one thread and readers always see a consistent state.
 *
 * <p>The replica is configured with an ordered list of member archives (see
 * {@link ReadReplicaConfig}). Recording positions are cluster-global - every
 * member records the same committed consensus log - so when the current source
 * dies the replica fails over to the next member and resumes the replay from
 * the position already applied, keeping the engine and ledger state. Only when
 * no reachable source's recording covers the applied position (source behind,
 * history purged) does it rebuild the state from the start of the log. A source
 * that delivers no fragments and no successful archive op within
 * {@link ReadReplicaConfig#livenessTimeoutMs()} is failed over.
 */
public final class ReadReplica implements AutoCloseable {

    private static final int FRAGMENT_LIMIT = 64;

    // Aeron's 128 KB socket receive default overflows under replay bursts; size
    // it above the largest term buffer so packets are never dropped by the OS.
    private static final int SOCKET_RCVBUF_LENGTH = 16 * 1024 * 1024;
    private static final int SOCKET_SNDBUF_LENGTH = 16 * 1024 * 1024;

    // Consecutive error-driven failovers at the same applied position before
    // the replica rebuilds from the log start instead of cycling sources: every
    // member replays the same committed prefix, so a fragment that throws on
    // one source throws on all of them and failover alone can never make
    // progress past it.
    private static final int MAX_STALE_FAILOVERS = 8;

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final String[] archiveControlChannels;
    private final int archiveControlStreamId;
    private final String localHost;
    private final ReadReplicaConfig config;
    private final CoreConfig coreConfig;
    private final MatchingEngine engine;
    private final CommandOutcome outcome;
    private final ReportGenerator reports;
    private final ReplicationHealth health = new ReplicationHealth();

    private OrderLedger ledger;
    private ReplicaCommandListener commandListener = ReplicaCommandListener.NONE;
    private LiveLogSubscriber liveLog;
    private AeronArchive archive;
    private int currentSource;
    private long appliedPosition;
    private long lastFailedPosition = -1L;
    private int staleFailovers;
    private long nextConnectMs;
    private long lastActivityMs;
    private long lastProbeMs;
    private String currentChannel;

    private SnapshotSubscriber snapshotSubscriber;
    private long snapshotLoadStartMs;
    private long rejectedSnapshotPosition = -1L;
    private LedgerRebuilder ledgerRebuilder;
    private boolean ledgerRebuildNeeded;
    private long nextSnapshotPollMs;
    private long nextLedgerRebuildMs;
    private long nextCheckpointMs;
    private long lastCheckpointPosition = -1L;

    public ReadReplica(final ReadReplicaConfig config, final CoreConfig coreConfig) {
        this.config = config;
        this.coreConfig = coreConfig;
        this.engine = new MatchingEngine(coreConfig, new CoreMetrics());
        this.outcome = new CommandOutcome(coreConfig.eventBufferCapacity());
        this.ledger = new OrderLedger(config.maxOrdersPerUser(), config.maxMarketTrades());
        this.localHost = config.localHost();
        this.reports = new ReportGenerator(engine);
        this.archiveControlChannels = config.archiveControlChannels();
        this.archiveControlStreamId = config.archiveControlStreamId();
        this.currentChannel = archiveControlChannels[0];

        MediaDriver driver = null;
        Aeron aeronClient = null;
        try {
            driver = MediaDriver.launch(new MediaDriver.Context()
                    .aeronDirectoryName(config.aeronDirectoryName())
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true)
                    .socketRcvbufLength(SOCKET_RCVBUF_LENGTH)
                    .socketSndbufLength(SOCKET_SNDBUF_LENGTH));
            aeronClient = Aeron.connect(new Aeron.Context().aeronDirectoryName(config.aeronDirectoryName()));
        } catch (final RuntimeException e) {
            closeQuietly(aeronClient, driver);
            throw e;
        }
        this.mediaDriver = driver;
        this.aeron = aeronClient;

        if (config.checkpointFile() != null && Files.exists(config.checkpointFile())) {
            try {
                final ReplicaCheckpoint.Data data = ReplicaCheckpoint.load(
                        config.checkpointFile(), engine, config.maxOrdersPerUser(), config.maxMarketTrades());
                this.ledger = data.ledger();
                this.appliedPosition = data.logPosition();
                this.currentSource = Math.floorMod(data.currentSource(), archiveControlChannels.length);
                this.lastCheckpointPosition = appliedPosition;
            } catch (final IOException | RuntimeException e) {
                // A corrupt checkpoint must not make the replica unconstructable:
                // fall back to a cold start (the consensus log is replayed in
                // full) and surface the failure through the health counters.
                // The load feeds records into the engine before the file is
                // fully read or validated, so a failure can leave partial or
                // invariant-violating state behind; clear it before the replay
                // (the same recovery SnapshotSubscriber applies to a corrupt
                // cluster snapshot). RuntimeException covers corrupt length
                // fields the ledger decode does not range-check.
                engine.clearState();
                health.recordCheckpointFailure();
                this.ledger = new OrderLedger(config.maxOrdersPerUser(), config.maxMarketTrades());
                this.appliedPosition = 0L;
                this.currentSource = 0;
                this.lastCheckpointPosition = -1L;
            }
        }
    }

    /** Advances replication by polling the live log; call repeatedly from one thread. */
    public int poll() {
        try {
            final long now = System.currentTimeMillis();
            ensureLiveLog();
            // Snapshot bootstrap / fast-forward runs even while the live log is
            // stopped for a load, so a cold start can load the cluster snapshot
            // before replaying history it can skip.
            pollForNewSnapshot(now);
            pollLedgerRebuild();
            if (liveLog == null) {
                health.markStale(currentChannel, appliedPosition);
                return 0;
            }
            if (now - lastActivityMs > config.livenessTimeoutMs()) {
                failover();
                return 0;
            }
            probeIfDue(now);
            // probeIfDue may fail the source over (setting liveLog null); a null
            // dereference here would re-trigger failover in the catch and advance
            // the source twice in one poll.
            if (liveLog == null) {
                return 0;
            }
            final int fragments = liveLog.poll(FRAGMENT_LIMIT);
            if (fragments > 0) {
                lastActivityMs = now;
                final long position = liveLog.lastPosition();
                if (position > appliedPosition) {
                    appliedPosition = position;
                }
            }
            health.markHealthy(currentChannel, appliedPosition);
            if (liveLog.isReplayEnded()) {
                // The bounded replay caught up to an idle tail; re-point a fresh
                // replay from the consumed position after a short backoff so
                // commits that land later are still followed.
                liveLog.close();
                liveLog = null;
                nextConnectMs = now + config.failoverBackoffMs();
            }
            writeCheckpointIfDue(now);
            return fragments;
        } catch (final RuntimeException e) {
            // Counted (not printed) so a poison fragment or internal error is
            // observable operationally without synchronous console I/O on the
            // poll thread.
            health.recordPollError();
            failoverWithoutProgress(appliedPosition);
            failover();
            return 0;
        }
    }

    // Escalates error-driven failovers that make no position progress into a
    // rebuild from the log start, so a fragment that throws on every source
    // cannot cycle the replica through its sources forever. Returns true when
    // the escalation fired. Package-private so the escalation is testable
    // without a cluster.
    boolean failoverWithoutProgress(final long position) {
        if (position == lastFailedPosition) {
            staleFailovers++;
        } else {
            lastFailedPosition = position;
            staleFailovers = 1;
        }
        if (staleFailovers >= MAX_STALE_FAILOVERS) {
            staleFailovers = 0;
            lastFailedPosition = -1L;
            resetReplication();
            health.recordRebuildFailure();
            return true;
        }
        return false;
    }

    /**
     * Periodically loads a newer service snapshot into the engine (advance-only
     * guard). The live log is stopped for the duration of the load (both feed
     * the same engine) and restarted from the snapshot position on completion. A
     * loaded snapshot fast-forwards the engine past the live-followed ledger, so
     * a ledger rebuild is scheduled to restore the full order history.
     */
    private void pollForNewSnapshot(final long now) {
        if (snapshotSubscriber == null) {
            // A rebuild needs the archive to itself (and the engine state is
            // frozen meanwhile), so snapshot polling waits for it to finish.
            if (now < nextSnapshotPollMs || archive == null || ledgerRebuilder != null || ledgerRebuildNeeded) {
                return;
            }
            snapshotSubscriber = new SnapshotSubscriber(engine, localHost);
            if (snapshotSubscriber.start(archive, Math.max(appliedPosition, rejectedSnapshotPosition))) {
                restartLiveLog();
            } else {
                snapshotSubscriber.close();
                snapshotSubscriber = null;
            }
            nextSnapshotPollMs = now + config.snapshotPollIntervalMs();
        }
        if (snapshotSubscriber != null) {
            snapshotSubscriber.poll(FRAGMENT_LIMIT);
            if (snapshotSubscriber.isComplete()) {
                finishSnapshotLoad();
            } else if (snapshotSubscriber.isLoadStarted()) {
                if (snapshotLoadStartMs == 0L) {
                    snapshotLoadStartMs = now;
                } else if (now - snapshotLoadStartMs > config.snapshotLoadTimeoutMs()) {
                    // A truncated or stalled snapshot load must not wedge the
                    // replica: abort and fail over to the next source.
                    snapshotLoadStartMs = 0L;
                    failover();
                }
            }
        }
    }

    private void finishSnapshotLoad() {
        snapshotLoadStartMs = 0L;
        switch (snapshotSubscriber.result()) {
            case LOADED -> {
                health.recordSnapshotLoaded();
                final long loaded = snapshotSubscriber.loadedLogPosition();
                if (loaded > appliedPosition) {
                    appliedPosition = loaded;
                    // The engine jumped past the ledger's coverage (the ledger is
                    // read-side-only and not in the cluster snapshot), so the full
                    // history must be replayed once to restore it.
                    ledgerRebuildNeeded = true;
                }
            }
            case CORRUPT -> {
                health.recordIntegrityFailure();
                // Remember the rejected snapshot's position so a later poll
                // never reloads the same corrupt recording (it would reset the
                // replica in a loop). Positions are cluster-global, so the
                // rejection holds across failovers.
                rejectedSnapshotPosition = Math.max(rejectedSnapshotPosition, snapshotSubscriber.loadedLogPosition());
                resetReplication();
            }
            default -> {
                // SKIPPED (nothing newer) or FAILED (transient): keep following.
            }
        }
        snapshotSubscriber.close();
        snapshotSubscriber = null;
        restartLiveLog();
    }

    /**
     * Advances the ledger rebuild and swaps in the complete ledger when done.
     * The live log stays stopped for the whole rebuild, so the applied position
     * is frozen at the snapshot position and the rebuild covers exactly the
     * prefix the engine holds; the live log restarted after the swap feeds the
     * engine and the new ledger the identical tail with no overlap.
     */
    private void pollLedgerRebuild() {
        if (!ledgerRebuildNeeded || System.currentTimeMillis() < nextLedgerRebuildMs) {
            return;
        }
        // The live log is off during a rebuild, so ensureLiveLog never runs and
        // the archive connection is owned here (a failover closes it).
        if (archive == null && !connectArchiveForRebuild()) {
            nextLedgerRebuildMs = System.currentTimeMillis() + config.failoverBackoffMs();
            return;
        }
        if (ledgerRebuilder == null) {
            ledgerRebuilder =
                    new LedgerRebuilder(coreConfig, localHost, config.maxOrdersPerUser(), config.maxMarketTrades());
            if (!ledgerRebuilder.start(archive, appliedPosition)) {
                ledgerRebuilder.close();
                ledgerRebuilder = null;
                health.recordRebuildFailure();
                nextLedgerRebuildMs = System.currentTimeMillis() + config.failoverBackoffMs();
                return;
            }
        }
        ledgerRebuilder.poll(FRAGMENT_LIMIT);
        if (ledgerRebuilder.replayLost()) {
            // The replay image closed before the prefix was covered; a partial
            // ledger is never swapped in. Retry after a backoff.
            ledgerRebuilder.close();
            ledgerRebuilder = null;
            health.recordRebuildFailure();
            nextLedgerRebuildMs = System.currentTimeMillis() + config.failoverBackoffMs();
            return;
        }
        if (ledgerRebuilder.isCaughtUp()) {
            ledger = ledgerRebuilder.ledger();
            ledgerRebuilder.close();
            ledgerRebuilder = null;
            ledgerRebuildNeeded = false;
            // The ledger just became complete; persist it immediately.
            writeCheckpoint();
            // Rebind the live path to the new ledger: the subscriber captures
            // its ledger at construction, so it must be restarted.
            restartLiveLog();
        }
    }

    /** Writes a checkpoint on the configured cadence, when the position advanced. */
    private void writeCheckpointIfDue(final long now) {
        if (config.checkpointFile() == null || now < nextCheckpointMs) {
            return;
        }
        nextCheckpointMs = now + config.checkpointIntervalMs();
        writeCheckpoint();
    }

    private void writeCheckpoint() {
        if (config.checkpointFile() == null || appliedPosition <= 0L || appliedPosition <= lastCheckpointPosition) {
            return;
        }
        try {
            ReplicaCheckpoint.save(config.checkpointFile(), engine, ledger, appliedPosition, currentSource);
            lastCheckpointPosition = appliedPosition;
        } catch (final IOException e) {
            // A checkpoint write failure must not kill the replica; it retries
            // on the next interval. The failure is counted (not printed) so it is
            // observable operationally without synchronous console I/O.
            health.recordCheckpointFailure();
        }
    }

    private void restartLiveLog() {
        if (liveLog != null) {
            liveLog.close();
            liveLog = null;
        }
        nextConnectMs = 0L;
    }

    /** Connects to any reachable source archive for a ledger rebuild. */
    private boolean connectArchiveForRebuild() {
        for (int attempt = 0; attempt < archiveControlChannels.length; attempt++) {
            final int idx = (currentSource + attempt) % archiveControlChannels.length;
            closeArchive();
            if (connectArchive(idx)) {
                currentSource = idx;
                lastActivityMs = System.currentTimeMillis();
                return true;
            }
        }
        closeArchive();
        return false;
    }

    private void ensureLiveLog() {
        // A snapshot load feeds the same engine, so the live log must not run
        // concurrently with it. A pending or running ledger rebuild also needs
        // the applied position frozen, so the live log stays off until the
        // rebuilt ledger is swapped in.
        if (liveLog != null
                || snapshotSubscriber != null
                || ledgerRebuildNeeded
                || System.currentTimeMillis() < nextConnectMs) {
            return;
        }
        boolean anyArchiveConnected = false;
        for (int attempt = 0; attempt < archiveControlChannels.length; attempt++) {
            final int idx = (currentSource + attempt) % archiveControlChannels.length;
            closeArchive();
            if (!connectArchive(idx)) {
                continue;
            }
            anyArchiveConnected = true;
            // Resume from the cluster-global applied position on ANY member: the
            // committed prefix is byte-identical everywhere, so the boundary is
            // valid on the new source and no rebuild is needed.
            final LiveLogSubscriber subscriber = new LiveLogSubscriber(
                    archive, engine, outcome, ledger, commandListener, appliedPosition, localHost);
            if (subscriber.connect()) {
                liveLog = subscriber;
                currentSource = idx;
                lastActivityMs = System.currentTimeMillis();
                health.markHealthy(currentChannel, appliedPosition);
                return;
            }
            subscriber.close();
        }
        // Every reachable archive rejected a replay from the applied position
        // (no recording covers it): the position is not servable on this quorum,
        // so rebuild the replicated state from the start of the log.
        if (anyArchiveConnected && appliedPosition > 0L) {
            resetReplication();
        }
        closeArchive();
        nextConnectMs = System.currentTimeMillis() + config.failoverBackoffMs();
        health.markStale(currentChannel, appliedPosition);
    }

    private boolean connectArchive(final int idx) {
        try {
            archive = AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .controlRequestChannel(archiveControlChannels[idx])
                    .controlRequestStreamId(archiveControlStreamId)
                    .controlResponseChannel("aeron:udp?endpoint=" + localHost + ":0")
                    .messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(config.archiveMessageTimeoutMs())));
            currentChannel = archiveControlChannels[idx];
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

    /**
     * Drops the current source and schedules a reconnect to the next member
     * after the failover backoff. The engine and ledger state are kept; the next
     * connect resumes from {@code appliedPosition}. An in-flight snapshot load is
     * aborted: the engine may hold partial loaded state, so it is rebuilt from
     * the log start on the new source.
     */
    private void failover() {
        if (liveLog != null) {
            liveLog.close();
            liveLog = null;
        }
        if (snapshotSubscriber != null) {
            final boolean midLoad = snapshotSubscriber.isLoadStarted();
            snapshotSubscriber.close();
            snapshotSubscriber = null;
            if (midLoad) {
                resetReplication();
            }
        }
        if (ledgerRebuilder != null) {
            // The rebuild is bound to the dead source's archive; it restarts on
            // the new source because ledgerRebuildNeeded stays set.
            ledgerRebuilder.close();
            ledgerRebuilder = null;
        }
        closeArchive();
        currentSource = (currentSource + 1) % archiveControlChannels.length;
        health.recordFailover();
        nextConnectMs = System.currentTimeMillis() + config.failoverBackoffMs();
        health.markStale(currentChannel, appliedPosition);
    }

    /**
     * Periodically probes the active archive so a silently dead source is
     * detected even when no fragments flow; a successful archive op counts as
     * activity, so an idle but healthy cluster never false-positives.
     */
    private void probeIfDue(final long now) {
        final long interval = Math.max(1L, config.livenessTimeoutMs() / 2);
        if (now - lastProbeMs < interval || archive == null || liveLog == null) {
            return;
        }
        lastProbeMs = now;
        try {
            archive.getRecordingPosition(liveLog.recordingId());
            lastActivityMs = now;
        } catch (final RuntimeException e) {
            failover();
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

    /** Replication health snapshot for operators. */
    public ReplicationHealth health() {
        return health;
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
        // Final checkpoint (bypasses the cadence) so a warm start resumes from
        // the last applied position - but only from a steady state. Mid snapshot
        // load the engine holds partial records; mid ledger rebuild the engine
        // is ahead of the still-pre-snapshot ledger and the rebuild flag is not
        // persisted, so a warm start could never repair either skew.
        final boolean midSnapshotLoad = snapshotSubscriber != null && snapshotSubscriber.isLoadStarted();
        if (!midSnapshotLoad && !ledgerRebuildNeeded && ledgerRebuilder == null) {
            writeCheckpoint();
        }
        if (snapshotSubscriber != null) {
            snapshotSubscriber.close();
            snapshotSubscriber = null;
        }
        if (ledgerRebuilder != null) {
            ledgerRebuilder.close();
            ledgerRebuilder = null;
        }
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
