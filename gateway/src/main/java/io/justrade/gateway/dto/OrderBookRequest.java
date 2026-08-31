package io.justrade.gateway.dto;

/** JSON body for {@code POST /api/v1/orderbook/{symbolId}/request}. */
public record OrderBookRequest(Long uid) {}
