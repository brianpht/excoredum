package com.exadbe.read.client;

import java.util.List;

/**
 * Result of a {@code TOTAL_CURRENCY_BALANCE} query: the per-currency value
 * conservation breakdown of the replicated state.
 */
public record TotalBalanceResult(long appliedPosition, List<Total> totals) {

    /** Conservation view for one currency. {@code total} equals the sum of the three components. */
    public record Total(int currency, long accountBalance, long reserved, long fees) {

        /** The conserved total: account balances plus reserved holds plus collected fees. */
        public long total() {
            return accountBalance + reserved + fees;
        }
    }
}
