package io.justrade.read.client;

import java.util.List;

/** Result of a {@code SINGLE_USER_REPORT} query: status, balances, and resting orders. */
public record UserReport(
        long uid,
        boolean exists,
        boolean suspended,
        long appliedPosition,
        List<Balance> balances,
        List<RestingOrder> orders) {

    /** One currency balance held by the reported user. */
    public record Balance(int currency, long balance) {}

    /** One resting order owned by the reported user. */
    public record RestingOrder(
            int symbolId, long orderId, boolean ask, long price, long size, long filled, long reserveBidPrice) {}
}
