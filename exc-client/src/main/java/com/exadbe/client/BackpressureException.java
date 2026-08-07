package com.exadbe.client;

/**
 * Thrown by {@link ExcClient#submit} when the in-flight command window is full.
 *
 * <p>Signals backpressure explicitly to the caller rather than silently dropping
 * the command. The caller should {@link ExcClient#poll} to drain
 * acknowledgements and retry.
 */
public final class BackpressureException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public BackpressureException(final String message) {
        super(message);
    }
}
