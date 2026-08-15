package com.exadbe.read;

/**
 * Aeron stream ids the read replica uses for ephemeral Archive replay delivery.
 *
 * <p>The ids MUST be distinct so a snapshot replay, a live-log replay, and a
 * ledger-rebuild replay can run concurrently on the replica's single embedded
 * media driver without their subscriptions colliding. They are delivery streams
 * for the replica's own subscriptions and are unrelated to the source recording
 * streams (consensus log 100, service snapshots).
 */
final class ReadStreams {

    /** Delivery stream for a service snapshot replay. */
    static final int SNAPSHOT_REPLAY = 42;

    /** Delivery stream for a consensus log (source stream 100) live replay. */
    static final int LIVE_LOG_REPLAY = 43;

    /** Delivery stream for the cold-start ledger-rebuild replay. */
    static final int LEDGER_REBUILD_REPLAY = 46;

    /** Source stream id of the consensus module log recording. */
    static final int CONSENSUS_LOG_STREAM_ID = 100;

    private ReadStreams() {}
}
