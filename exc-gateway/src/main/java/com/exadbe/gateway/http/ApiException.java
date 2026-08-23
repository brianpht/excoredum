package com.exadbe.gateway.http;

/**
 * A gateway error carrying an HTTP status. Thrown/carried by the read and write
 * bridges and translated to a JSON error response by the router.
 */
public final class ApiException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int status;

    public ApiException(final int status, final String message) {
        super(message);
        this.status = status;
    }

    public int status() {
        return status;
    }

    public static ApiException badRequest(final String message) {
        return new ApiException(400, message);
    }

    public static ApiException notFound(final String message) {
        return new ApiException(404, message);
    }

    public static ApiException conflict(final String message) {
        return new ApiException(409, message);
    }

    public static ApiException timeout(final String message) {
        return new ApiException(504, message);
    }

    public static ApiException server(final String message) {
        return new ApiException(500, message);
    }
}
