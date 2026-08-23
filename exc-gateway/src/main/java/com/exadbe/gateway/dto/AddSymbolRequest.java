package com.exadbe.gateway.dto;

/** JSON body for the admin {@code POST /api/v1/symbols} endpoint. */
public record AddSymbolRequest(
        int symbolId,
        int baseCurrency,
        int quoteCurrency,
        long baseScaleK,
        long quoteScaleK,
        long takerFee,
        long makerFee) {}
