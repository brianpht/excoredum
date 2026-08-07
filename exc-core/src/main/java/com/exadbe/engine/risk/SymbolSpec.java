package com.exadbe.engine.risk;

/** Immutable spot symbol specification: currencies and integer scale factors. */
public final class SymbolSpec {

    private final int symbolId;
    private final int baseCurrency;
    private final int quoteCurrency;
    private final long baseScaleK;
    private final long quoteScaleK;

    public SymbolSpec(
            final int symbolId,
            final int baseCurrency,
            final int quoteCurrency,
            final long baseScaleK,
            final long quoteScaleK) {
        this.symbolId = symbolId;
        this.baseCurrency = baseCurrency;
        this.quoteCurrency = quoteCurrency;
        this.baseScaleK = baseScaleK;
        this.quoteScaleK = quoteScaleK;
    }

    public int symbolId() {
        return symbolId;
    }

    public int baseCurrency() {
        return baseCurrency;
    }

    public int quoteCurrency() {
        return quoteCurrency;
    }

    public long baseScaleK() {
        return baseScaleK;
    }

    public long quoteScaleK() {
        return quoteScaleK;
    }
}
