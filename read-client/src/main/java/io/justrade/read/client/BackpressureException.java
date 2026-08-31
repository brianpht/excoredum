package io.justrade.read.client;

/**
 * Thrown by {@link ReadClient#submit} when the in-flight query window is full.
 *
 * <p>Signals backpressure explicitly to the caller rather than silently dropping
 * the query. The caller should {@link ReadClient#poll} to drain responses and
 * free window slots, then retry.
 */
public final class BackpressureException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public BackpressureException(final String message) {
        super(message);
    }
}
