package com.exadbe.read.order;

/** One market trade as recorded from a replicated TRADE event. */
public record MarketTrade(
        long timestamp, int symbolId, long price, long size, long makerOrderId, long makerUid, long takerUid) {}
