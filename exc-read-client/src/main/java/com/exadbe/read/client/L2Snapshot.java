package com.exadbe.read.client;

import java.util.List;

/** Result of an {@code L2_ORDER_BOOK} query: best-first price levels per side. */
public record L2Snapshot(int symbolId, boolean found, long appliedPosition, List<Level> asks, List<Level> bids) {

    /** One price level: aggregated volume and resting order count. */
    public record Level(long price, long size, int orders) {}
}
