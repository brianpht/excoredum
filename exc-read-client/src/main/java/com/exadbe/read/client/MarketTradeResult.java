package com.exadbe.read.client;

/** One market trade from the replicated tape. */
public record MarketTradeResult(
        long timestamp, int symbolId, long price, long size, long makerOrderId, long makerUid, long takerUid) {}
