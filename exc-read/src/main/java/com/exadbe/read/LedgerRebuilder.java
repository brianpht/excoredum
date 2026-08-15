package com.exadbe.read;

import com.exadbe.config.CoreConfig;
import com.exadbe.core.CommandOutcome;
import com.exadbe.engine.MatchingEngine;
import com.exadbe.read.order.OrderLedger;
import com.exadbe.telemetry.CoreMetrics;
import io.aeron.archive.client.AeronArchive;

/**
 * Rebuilds the {@link OrderLedger} from the full consensus log after the engine
 * was fast-forwarded by a snapshot load. The cluster snapshot contains engine
 * state only - the ledger is read-side-only - so a cold start that bootstraps
 * the engine from a snapshot must still replay the whole log once to restore the
 * complete order history and trade tape.
 *
 * <p>It runs a throwaway {@link MatchingEngine} (whose outcomes drive the
 * ledger) on its own replay stream, polled from the replica's single thread and
 * interleaved with query serving. On completion the replica swaps in the rebuilt
 * ledger. Poll-driven; {@link #isCaughtUp()} signals completion.
 */
final class LedgerRebuilder implements AutoCloseable {

    private final CoreConfig coreConfig;
    private final String localHost;
    private final OrderLedger ledger = new OrderLedger();
    private MatchingEngine engine;
    private CommandOutcome outcome;
    private LiveLogSubscriber subscriber;
    private long lastPosition;

    LedgerRebuilder(final CoreConfig coreConfig, final String localHost) {
        this.coreConfig = coreConfig;
        this.localHost = localHost;
    }

    /** Starts a full-log replay from position 0 on the given archive. */
    boolean start(final AeronArchive archive) {
        this.engine = new MatchingEngine(coreConfig, new CoreMetrics());
        this.outcome = new CommandOutcome(coreConfig.eventBufferCapacity());
        this.subscriber = new LiveLogSubscriber(
                archive,
                engine,
                outcome,
                ledger,
                ReplicaCommandListener.NONE,
                0L,
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

    /** Whether the rebuild replay caught up to the live head of the recording. */
    boolean isCaughtUp() {
        return subscriber != null && subscriber.isCaughtUp();
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
