package com.exadbe.client;

/**
 * Reusable holder for one L2 order-book snapshot delivered on the session
 * egress ({@code L2MarketData}). Filled on the client's polling thread
 * immediately before {@link OrderBookListener#onOrderBook} is invoked; the
 * contents are overwritten by the next snapshot, so a handler that needs the
 * data beyond the callback must copy it.
 */
public final class OrderBookSnapshot {

    private static final int INITIAL_DEPTH = 16;

    private long commandIdHi;
    private long commandIdLo;
    private int symbolId;

    private long[] askPrices = new long[INITIAL_DEPTH];
    private long[] askVolumes = new long[INITIAL_DEPTH];
    private int[] askOrders = new int[INITIAL_DEPTH];
    private int askDepth;

    private long[] bidPrices = new long[INITIAL_DEPTH];
    private long[] bidVolumes = new long[INITIAL_DEPTH];
    private int[] bidOrders = new int[INITIAL_DEPTH];
    private int bidDepth;

    public long commandIdHi() {
        return commandIdHi;
    }

    public long commandIdLo() {
        return commandIdLo;
    }

    public int symbolId() {
        return symbolId;
    }

    public int askDepth() {
        return askDepth;
    }

    public long askPrice(final int index) {
        return askPrices[index];
    }

    public long askVolume(final int index) {
        return askVolumes[index];
    }

    public int askOrders(final int index) {
        return askOrders[index];
    }

    public int bidDepth() {
        return bidDepth;
    }

    public long bidPrice(final int index) {
        return bidPrices[index];
    }

    public long bidVolume(final int index) {
        return bidVolumes[index];
    }

    public int bidOrders(final int index) {
        return bidOrders[index];
    }

    void begin(final long commandIdHi, final long commandIdLo, final int symbolId) {
        this.commandIdHi = commandIdHi;
        this.commandIdLo = commandIdLo;
        this.symbolId = symbolId;
        this.askDepth = 0;
        this.bidDepth = 0;
    }

    void addAsk(final long price, final long volume, final int orders) {
        if (askDepth == askPrices.length) {
            growAsks();
        }
        askPrices[askDepth] = price;
        askVolumes[askDepth] = volume;
        askOrders[askDepth] = orders;
        askDepth++;
    }

    void addBid(final long price, final long volume, final int orders) {
        if (bidDepth == bidPrices.length) {
            growBids();
        }
        bidPrices[bidDepth] = price;
        bidVolumes[bidDepth] = volume;
        bidOrders[bidDepth] = orders;
        bidDepth++;
    }

    // Cold path: a deeper book than any snapshot seen so far.
    private void growAsks() {
        final int oldLength = askPrices.length;
        final int newLength = oldLength * 2;
        askPrices = copyOf(askPrices, newLength);
        askVolumes = copyOf(askVolumes, newLength);
        askOrders = copyOf(askOrders, newLength);
    }

    private void growBids() {
        final int oldLength = bidPrices.length;
        final int newLength = oldLength * 2;
        bidPrices = copyOf(bidPrices, newLength);
        bidVolumes = copyOf(bidVolumes, newLength);
        bidOrders = copyOf(bidOrders, newLength);
    }

    private static long[] copyOf(final long[] source, final int newLength) {
        final long[] larger = new long[newLength];
        System.arraycopy(source, 0, larger, 0, source.length);
        return larger;
    }

    private static int[] copyOf(final int[] source, final int newLength) {
        final int[] larger = new int[newLength];
        System.arraycopy(source, 0, larger, 0, source.length);
        return larger;
    }
}
