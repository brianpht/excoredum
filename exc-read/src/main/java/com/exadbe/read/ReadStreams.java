package com.exadbe.read;

/**
 * Aeron stream ids the read replica uses for ephemeral Archive replay delivery.
 * Distinct from the source recording streams (consensus log 100) so the replica's
 * own subscriptions do not collide.
 */
final class ReadStreams {

    /** Delivery stream for a consensus log (source stream 100) live replay. */
    static final int LIVE_LOG_REPLAY = 43;

    /** Source stream id of the consensus module log recording. */
    static final int CONSENSUS_LOG_STREAM_ID = 100;

    private ReadStreams() {}
}
