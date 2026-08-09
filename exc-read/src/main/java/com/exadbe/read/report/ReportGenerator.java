package com.exadbe.read.report;

import com.exadbe.engine.MatchingEngine;
import com.exadbe.engine.risk.DirectExchangeRisk;
import com.exadbe.engine.risk.SymbolSpec;

/**
 * Assembles read-side reports over a replica's {@link MatchingEngine}. Runs on
 * the replica's single poll thread, the same thread that advances replication,
 * so every report sees a consistent state and the engine's stores are never
 * touched concurrently.
 */
public final class ReportGenerator {

    private final MatchingEngine engine;

    public ReportGenerator(final MatchingEngine engine) {
        this.engine = engine;
    }

    /** Balances and resting orders for {@code uid}. */
    public SingleUserReport singleUser(final long uid) {
        final SingleUserReport report = new SingleUserReport(uid, engine.userExists(uid));
        engine.forEachBalance((u, currency, balance) -> {
            if (u == uid) {
                report.putBalance(currency, balance);
            }
        });
        engine.forEachOrder((symbolId, orderId, ask, price, size, filled, reserveBidPrice, u, timestamp) -> {
            if (u == uid) {
                report.addOrder(
                        new SingleUserReport.OrderLine(symbolId, orderId, ask, price, size, filled, reserveBidPrice));
            }
        });
        return report;
    }

    /** Per-currency total of all balances plus funds reserved by resting orders. */
    public TotalCurrencyBalance totalCurrencyBalance() {
        final TotalCurrencyBalance total = new TotalCurrencyBalance();
        engine.forEachBalance((uid, currency, balance) -> total.add(currency, balance));
        engine.forEachOrder((symbolId, orderId, ask, price, size, filled, reserveBidPrice, uid, timestamp) -> {
            final SymbolSpec spec = engine.symbolSpec(symbolId);
            if (spec == null) {
                return;
            }
            final long remaining = size - filled;
            if (ask) {
                total.add(spec.baseCurrency(), DirectExchangeRisk.askHold(spec, remaining));
            } else {
                total.add(spec.quoteCurrency(), DirectExchangeRisk.bidHold(spec, remaining, reserveBidPrice));
            }
        });
        return total;
    }

    /** Deterministic fingerprint of the full replicated state. */
    public long stateHash() {
        return engine.stateHash();
    }
}
