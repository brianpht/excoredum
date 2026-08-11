package com.exadbe.gateway.core;

/**
 * Gateway view of a spot symbol: the code clients trade by, the core integer
 * id, its base and quote assets, and the scaled operands submitted with
 * ADD_SYMBOL. Spot only: margin fields are rejected at registration.
 */
public final class GatewaySymbolSpec {

    /** Lifecycle: registered locally, not yet confirmed by the core. */
    public static final int STATUS_NEW = 0;

    /** Lifecycle: ADD_SYMBOL acknowledged with SUCCESS. */
    public static final int STATUS_ACTIVE = 1;

    private final int symbolId;
    private final String symbolCode;
    private final String symbolType;
    private final GatewayAssetSpec baseAsset;
    private final GatewayAssetSpec quoteCurrency;
    private final long baseScaleK;
    private final long quoteScaleK;
    private final long takerFee;
    private final long makerFee;
    private int status;

    public GatewaySymbolSpec(
            final int symbolId,
            final String symbolCode,
            final String symbolType,
            final GatewayAssetSpec baseAsset,
            final GatewayAssetSpec quoteCurrency,
            final long baseScaleK,
            final long quoteScaleK,
            final long takerFee,
            final long makerFee,
            final int status) {
        this.symbolId = symbolId;
        this.symbolCode = symbolCode;
        this.symbolType = symbolType;
        this.baseAsset = baseAsset;
        this.quoteCurrency = quoteCurrency;
        this.baseScaleK = baseScaleK;
        this.quoteScaleK = quoteScaleK;
        this.takerFee = takerFee;
        this.makerFee = makerFee;
        this.status = status;
    }

    public int symbolId() {
        return symbolId;
    }

    public String symbolCode() {
        return symbolCode;
    }

    public String symbolType() {
        return symbolType;
    }

    public GatewayAssetSpec baseAsset() {
        return baseAsset;
    }

    public GatewayAssetSpec quoteCurrency() {
        return quoteCurrency;
    }

    /** Lot size in base asset units, as submitted to ADD_SYMBOL. */
    public long baseScaleK() {
        return baseScaleK;
    }

    /** Step size in quote currency units, as submitted to ADD_SYMBOL. */
    public long quoteScaleK() {
        return quoteScaleK;
    }

    /** Per-lot taker fee in quote currency units. */
    public long takerFee() {
        return takerFee;
    }

    /** Per-lot maker fee in quote currency units. */
    public long makerFee() {
        return makerFee;
    }

    public int status() {
        return status;
    }

    public void status(final int value) {
        this.status = value;
    }
}
