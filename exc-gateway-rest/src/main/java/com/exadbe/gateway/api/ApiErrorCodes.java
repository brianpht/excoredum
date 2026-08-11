package com.exadbe.gateway.api;

/**
 * Gateway-level error codes carried in the {@code gatewayResultCode} envelope
 * field, mirroring the reference exchange-gateway-rest numbering.
 */
public enum ApiErrorCodes {
    SYMBOL_ALREADY_EXISTS(1000, "symbol already exists"),
    UNKNOWN_BASE_ASSET(1001, "unknown base asset"),
    UNKNOWN_QUOTE_CURRENCY(1002, "unknown quote currency"),
    ASSET_ALREADY_EXISTS(1003, "asset already exists"),
    UNKNOWN_CURRENCY(1004, "unknown currency"),
    PRECISION_IS_TOO_HIGH(1005, "precision is too high, reduce precision"),
    UNKNOWN_SYMBOL(1006, "unknown symbol"),
    UNKNOWN_SYMBOL_404(1007, "symbol not found"),
    INVALID_CONFIGURATION(1008, "invalid configuration"),
    INVALID_PRICE(1009, "invalid price"),
    UNKNOWN_USER_404(1010, "unknown user"),
    INVALID_BODY(1011, "invalid request body"),
    INVALID_SIZE(1012, "invalid size"),
    UNKNOWN_ROUTE(1013, "unknown route");

    public final int gatewayErrorCode;
    public final String errorDescription;

    ApiErrorCodes(final int gatewayErrorCode, final String errorDescription) {
        this.gatewayErrorCode = gatewayErrorCode;
        this.errorDescription = errorDescription;
    }
}
