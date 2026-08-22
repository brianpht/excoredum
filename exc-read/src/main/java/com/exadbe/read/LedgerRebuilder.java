package com.exadbe.read;

import com.exadbe.config.CoreConfig;
import com.exadbe.core.CommandOutcome;
import com.exadbe.engine.MatchingEngine;
import com.exadbe.read.order.OrderLedger;
import com.exadbe.telemetry.CoreMetrics;
import io.aeron.archive.client.AeronArchive;

/**
 * Rebuilds the {@link OrderLedger} from the consensus log after the engine
 * was fast-forwarded by a snapshot load. The cluster snapshot contains engine
 * state only - the ledger is read-side-only - so a cold start that bootstraps
 * the engine from a snapshot must still replay the log once to restore the
 * complete order history and trade tape.
 *
 * <p>The rebuild replays exactly the prefix the replica's engine has applied
 * (log start through the frozen applied position) and stops at that boundary,
 * so the swapped-in ledger aligns with the engine command-for-command; the live
 * log then resumes from the same position and feeds both without overlap. It
 * runs a throwaway {@link MatchingEngine} (whose outcomes drive the ledger) on
 * its own replay stream, polled from the replica's single thread and
 * interleaved with query serving. Poll-driven; {@link #isCaughtUp()} signals
 * completion.
 */
final class LedgerRebuilder implements AutoCloseable {

    private final CoreConfig coreConfig;
    private final String localHost;
    private final OrderLedger ledger = new OrderLedger();
    private MatchingEngine engine;
    private CommandOutcome outcome;
    private LiveLogSubscriber subscriber;
    private long targetPosition;
    private long lastPosition;

    LedgerRebuilder(final CoreConfig coreConfig, final String localHost) {
        this.coreConfig = coreConfig;
        this.localHost = localHost;
    }

    /**
     * Starts a replay of the prefix {@code [0, targetPosition]} on the given
     * archive; {@code targetPosition} is the replica's applied position, which
     * stays frozen while the rebuild runs.
     */
    boolean start(final AeronArchive archive, final long targetPosition) {
        this.engine = new MatchingEngine(coreConfig, new CoreMetrics());
        this.outcome = new CommandOutcome(coreConfig.eventBufferCapacity());
        this.targetPosition = targetPosition;
        this.subscriber = new LiveLogSubscriber(
                archive,
                engine,
                outcome,
                ledger,
                ReplicaCommandListener.NONE,
                0L,
                targetPosition,
                localHost,
                ReadStreams.LEDGER_REBUILD_REPLAY);
        this.lastPosition = 0L;
        return subscriber.connect();
    }

    /** Advances the rebuild replay; call from the replica's polling thread. */
    int poll(final int limit) {
        if (subscriber == null) {
            return 0;
        }
        final int fragments = subscriber.poll(limit);
        lastPosition = subscriber.lastPosition();
        return fragments;
    }

    /**
     * Whether the rebuild covered the whole target prefix: either the replay
     * delivered a fragment past the boundary or consumed exactly up to it.
     */
    boolean isCaughtUp() {
        return subscriber != null && (subscriber.reachedStop() || lastPosition >= targetPosition);
    }

    /**
     * Whether the replay image closed before the target prefix was covered
     * (source died or the recording was cut short). The rebuild must restart;
     * the partial ledger is never swapped in.
     */
    boolean replayLost() {
        return subscriber != null && !isCaughtUp() && subscriber.isReplayEnded();
    }

    long lastPosition() {
        return lastPosition;
    }

    /** The fully rebuilt ledger (complete once {@link #isCaughtUp()}). */
    OrderLedger ledger() {
        return ledger;
    }

    @Override
    public void close() {
        if (subscriber != null) {
            subscriber.close();
            subscriber = null;
        }
    }
}
