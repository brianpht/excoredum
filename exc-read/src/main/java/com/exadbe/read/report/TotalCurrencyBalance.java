package com.exadbe.read.report;

import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongLongConsumer;

/**
 * Per-currency conservation view: the total of every account balance plus the
 * funds still reserved by resting orders. Because reserving an order only moves
 * value from a balance into a hold (and fees accrue to the fee account, uid 0),
 * this total is invariant across trades for each currency, so it verifies value
 * conservation. Assembled on the read replica; heap allocation is acceptable.
 */
public final class TotalCurrencyBalance {

    private static final long MISSING = Long.MIN_VALUE;

    // Currency ids are widened to long keys; Agrona has no Int2Long map.
    private final Long2LongHashMap totals = new Long2LongHashMap(MISSING);

    void add(final int currency, final long amount) {
        final long current = totals.get(currency);
        totals.put(currency, (current == MISSING ? 0L : current) + amount);
    }

    /** The conserved total (balances plus reserved holds) for {@code currency}. */
    public long total(final int currency) {
        final long value = totals.get(currency);
        return value == MISSING ? 0L : value;
    }

    /** Emits every currency (as a long key) and its conserved total. */
    public void forEach(final LongLongConsumer consumer) {
        totals.forEachLong(consumer);
    }

    /** Number of distinct currencies with a non-empty total. */
    public int currencyCount() {
        return totals.size();
    }
}
