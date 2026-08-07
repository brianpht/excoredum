package com.exadbe.engine.orderbook;

/**
 * Reusable holder for an L2 order-book snapshot. Fixed-capacity arrays sized for
 * the deepest supported request; a single instance is owned by the engine and
 * refilled per {@code ORDER_BOOK_REQUEST}, so serving L2 allocates nothing.
 */
public final class L2View {

    private final long[] askPrice;
    private final long[] askVolume;
    private final int[] askOrders;
    private final long[] bidPrice;
    private final long[] bidVolume;
    private final int[] bidOrders;

    private int askDepth;
    private int bidDepth;

    public L2View(final int maxLevels) {
        this.askPrice = new long[maxLevels];
        this.askVolume = new long[maxLevels];
        this.askOrders = new int[maxLevels];
        this.bidPrice = new long[maxLevels];
        this.bidVolume = new long[maxLevels];
        this.bidOrders = new int[maxLevels];
    }

    public int maxLevels() {
        return askPrice.length;
    }

    public void reset() {
        askDepth = 0;
        bidDepth = 0;
    }

    void addAsk(final long price, final long volume, final int orders) {
        askPrice[askDepth] = price;
        askVolume[askDepth] = volume;
        askOrders[askDepth] = orders;
        askDepth++;
    }

    void addBid(final long price, final long volume, final int orders) {
        bidPrice[bidDepth] = price;
        bidVolume[bidDepth] = volume;
        bidOrders[bidDepth] = orders;
        bidDepth++;
    }

    public int askDepth() {
        return askDepth;
    }

    public int bidDepth() {
        return bidDepth;
    }

    public long askPrice(final int i) {
        return askPrice[i];
    }

    public long askVolume(final int i) {
        return askVolume[i];
    }

    public int askOrders(final int i) {
        return askOrders[i];
    }

    public long bidPrice(final int i) {
        return bidPrice[i];
    }

    public long bidVolume(final int i) {
        return bidVolume[i];
    }

    public int bidOrders(final int i) {
        return bidOrders[i];
    }
}
