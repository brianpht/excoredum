package com.exadbe.read.report;

import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongLongConsumer;

/**
 * Per-currency conservation view: the total of every account balance plus the
 * funds still reserved by resting orders. Because reserving an order only moves
 * value from a balance into a hold (and fees accrue to the fee account, uid 0),
 * this total is invariant across trades for each currency, so it verifies value
 * conservation. The total is also broken out into client account balances,
 * collected fees, and reserved order balances, mirroring exchange-core's
 * TotalCurrencyBalanceReport. Assembled on the read replica; heap allocation is
 * acceptable.
 */
public final class TotalCurrencyBalance {

    private static final long MISSING = Long.MIN_VALUE;

    // Currency ids are widened to long keys; Agrona has no Int2Long map.
    private final Long2LongHashMap totals = new Long2LongHashMap(MISSING);
    private final Long2LongHashMap accountBalances = new Long2LongHashMap(MISSING);
    private final Long2LongHashMap fees = new Long2LongHashMap(MISSING);
    private final Long2LongHashMap ordersBalances = new Long2LongHashMap(MISSING);

    void addAccountBalance(final int currency, final long amount) {
        addTo(accountBalances, currency, amount);
        addTo(totals, currency, amount);
    }

    void addFee(final int currency, final long amount) {
        addTo(fees, currency, amount);
        addTo(totals, currency, amount);
    }

    void addOrderHold(final int currency, final long amount) {
        addTo(ordersBalances, currency, amount);
        addTo(totals, currency, amount);
    }

    /** The conserved total (client balances, fees, and reserved holds) for {@code currency}. */
    public long total(final int currency) {
        return get(totals, currency);
    }

    /** The sum of client (non-fee) account balances for {@code currency}. */
    public long accountBalances(final int currency) {
        return get(accountBalances, currency);
    }

    /** The fees collected in {@code currency} (the fee account, uid 0). */
    public long fees(final int currency) {
        return get(fees, currency);
    }

    /** The funds reserved by resting orders in {@code currency}. */
    public long ordersBalances(final int currency) {
        return get(ordersBalances, currency);
    }

    /** Emits every currency (as a long key) and its conserved total. */
    public void forEach(final LongLongConsumer consumer) {
        totals.forEachLong(consumer);
    }

    /** Number of distinct currencies with a non-empty total. */
    public int currencyCount() {
        return totals.size();
    }

    private static void addTo(final Long2LongHashMap map, final int currency, final long amount) {
        final long current = map.get(currency);
        map.put(currency, (current == MISSING ? 0L : current) + amount);
    }

    private static long get(final Long2LongHashMap map, final int currency) {
        final long value = map.get(currency);
        return value == MISSING ? 0L : value;
    }
}
