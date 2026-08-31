package io.justrade.engine.risk;

/** Immutable spot symbol specification: currencies, integer scale factors, and fees. */
public final class SymbolSpec {

    private final int symbolId;
    private final int baseCurrency;
    private final int quoteCurrency;
    private final long baseScaleK;
    private final long quoteScaleK;
    private final long takerFee;
    private final long makerFee;

    public SymbolSpec(
            final int symbolId,
            final int baseCurrency,
            final int quoteCurrency,
            final long baseScaleK,
            final long quoteScaleK,
            final long takerFee,
            final long makerFee) {
        this.symbolId = symbolId;
        this.baseCurrency = baseCurrency;
        this.quoteCurrency = quoteCurrency;
        this.baseScaleK = baseScaleK;
        this.quoteScaleK = quoteScaleK;
        this.takerFee = takerFee;
        this.makerFee = makerFee;
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

    /** Fee charged per lot to a taker, in quote currency units. */
    public long takerFee() {
        return takerFee;
    }

    /** Fee charged per lot to a maker, in quote currency units. */
    public long makerFee() {
        return makerFee;
    }
}
