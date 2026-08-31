package io.justrade.read.client;

/** Thrown when a query response did not arrive within the configured retry budget. */
public final class QueryTimeoutException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public QueryTimeoutException(final String message) {
        super(message);
    }
}
