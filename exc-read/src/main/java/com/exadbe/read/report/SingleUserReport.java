package com.exadbe.read.report;

import java.util.ArrayList;
import java.util.List;
import org.agrona.collections.Long2LongHashMap;

/**
 * A point-in-time view of one user's account: per-currency balances and every
 * resting order the user still owns. Assembled on the read replica, so heap
 * allocation is acceptable here.
 */
public final class SingleUserReport {

    private static final long MISSING = Long.MIN_VALUE;

    private final long uid;
    private final boolean exists;

    // Currency ids are widened to long keys; Agrona has no Int2Long map.
    private final Long2LongHashMap balances = new Long2LongHashMap(MISSING);
    private final List<OrderLine> orders = new ArrayList<>();

    public SingleUserReport(final long uid, final boolean exists) {
        this.uid = uid;
        this.exists = exists;
    }

    void putBalance(final int currency, final long balance) {
        balances.put(currency, balance);
    }

    void addOrder(final OrderLine order) {
        orders.add(order);
    }

    public long uid() {
        return uid;
    }

    /** Whether the account exists on the replicated state. */
    public boolean exists() {
        return exists;
    }

    /** The balance for {@code currency}, or {@code 0} if the user holds none. */
    public long balance(final int currency) {
        final long value = balances.get(currency);
        return value == MISSING ? 0L : value;
    }

    /** All resting orders owned by the user, in deterministic engine order. */
    public List<OrderLine> orders() {
        return orders;
    }

    /** One resting order belonging to the reported user. */
    public record OrderLine(
            int symbolId, long orderId, boolean ask, long price, long size, long filled, long reserveBidPrice) {}
}
