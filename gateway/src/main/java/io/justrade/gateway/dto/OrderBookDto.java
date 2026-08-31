package io.justrade.gateway.dto;

import java.util.List;

/** An L2 order-book snapshot shaped for the UI (read-side {@code orderBook}). */
public record OrderBookDto(int symbolId, boolean found, long appliedPosition, List<Level> asks, List<Level> bids) {

    /** One price level: aggregated size and resting order count. */
    public record Level(long price, long size, int orders) {}
}
