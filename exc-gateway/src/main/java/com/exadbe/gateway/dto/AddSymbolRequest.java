package com.exadbe.gateway.dto;

/** JSON body for the admin {@code POST /api/v1/symbols} endpoint. Fees default to 0 when absent. */
public record AddSymbolRequest(
        Integer symbolId,
        Integer baseCurrency,
        Integer quoteCurrency,
        Long baseScaleK,
        Long quoteScaleK,
        long takerFee,
        long makerFee) {}
