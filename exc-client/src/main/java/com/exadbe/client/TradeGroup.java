package com.exadbe.client;

/**
 * Reusable holder for the fills one taker command produced, aggregated by
 * command id - the client-side view of exchange-core's grouped trade event.
 * Filled on the client's polling thread and passed to
 * {@link TradeGroupListener#onTradeGroup} once the command's event stream is
 * complete; the contents are overwritten by the next group, so a handler that
 * needs the data beyond the callback must copy it.
 */
public final class TradeGroup {

    private static final int INITIAL_FILLS = 16;

    private long commandIdHi;
    private long commandIdLo;
    private int symbolId;
    private long takerUid;
    private long totalVolume;

    private long[] makerOrderIds = new long[INITIAL_FILLS];
    private long[] makerUids = new long[INITIAL_FILLS];
    private long[] prices = new long[INITIAL_FILLS];
    private long[] sizes = new long[INITIAL_FILLS];
    private boolean[] makerCompleted = new boolean[INITIAL_FILLS];
    private int fillCount;

    public long commandIdHi() {
        return commandIdHi;
    }

    public long commandIdLo() {
        return commandIdLo;
    }

    public int symbolId() {
        return symbolId;
    }

    public long takerUid() {
        return takerUid;
    }

    /** Sum of the fill sizes in this group. */
    public long totalVolume() {
        return totalVolume;
    }

    public int fillCount() {
        return fillCount;
    }

    public long makerOrderId(final int index) {
        return makerOrderIds[index];
    }

    public long makerUid(final int index) {
        return makerUids[index];
    }

    public long price(final int index) {
        return prices[index];
    }

    public long size(final int index) {
        return sizes[index];
    }

    public boolean makerCompleted(final int index) {
        return makerCompleted[index];
    }

    void begin(final long commandIdHi, final long commandIdLo, final int symbolId, final long takerUid) {
        this.commandIdHi = commandIdHi;
        this.commandIdLo = commandIdLo;
        this.symbolId = symbolId;
        this.takerUid = takerUid;
        this.totalVolume = 0L;
        this.fillCount = 0;
    }

    void clear() {
        this.fillCount = 0;
        this.totalVolume = 0L;
    }

    void addFill(
            final long makerOrderId,
            final long makerUid,
            final long price,
            final long size,
            final boolean makerCompleted) {
        if (fillCount == makerOrderIds.length) {
            grow();
        }
        makerOrderIds[fillCount] = makerOrderId;
        makerUids[fillCount] = makerUid;
        prices[fillCount] = price;
        sizes[fillCount] = size;
        this.makerCompleted[fillCount] = makerCompleted;
        fillCount++;
        totalVolume += size;
    }

    // Cold path: a single command swept more makers than any group seen so far.
    private void grow() {
        final int oldLength = makerOrderIds.length;
        final int newLength = oldLength * 2;
        makerOrderIds = copyOf(makerOrderIds, newLength);
        makerUids = copyOf(makerUids, newLength);
        prices = copyOf(prices, newLength);
        sizes = copyOf(sizes, newLength);
        final boolean[] largerCompleted = new boolean[newLength];
        System.arraycopy(makerCompleted, 0, largerCompleted, 0, oldLength);
        makerCompleted = largerCompleted;
    }

    private static long[] copyOf(final long[] source, final int newLength) {
        final long[] larger = new long[newLength];
        System.arraycopy(source, 0, larger, 0, source.length);
        return larger;
    }
}
