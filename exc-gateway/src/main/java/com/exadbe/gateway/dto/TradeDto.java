package com.exadbe.gateway.dto;

/** One trade from the replicated tape (read-side {@code marketTrades} / {@code userTrades}). */
public record TradeDto(
        long timestamp, int symbolId, long price, long size, long makerOrderId, long makerUid, long takerUid) {}
