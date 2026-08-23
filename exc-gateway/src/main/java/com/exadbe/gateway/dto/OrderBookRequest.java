package com.exadbe.gateway.dto;

/** JSON body for {@code POST /api/v1/orderbook/{symbolId}/request}. */
public record OrderBookRequest(long uid) {}
