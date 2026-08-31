package io.justrade.xcorebench;

/**
 * A deterministic, replayable command sequence as parallel primitive arrays, so
 * both engines iterate the exact same bytes with zero per-command allocation.
 *
 * <p>The first {@code fillCount} commands build the book to its target depth;
 * the remainder is the measured benchmark phase (both phases are replayed, as
 * exchange-core's own ITOrderBookBase does).
 */
public final class Workload {

    public static final byte PLACE = 0;
    public static final byte CANCEL = 1;
    public static final byte MOVE = 2;
    public static final byte REDUCE = 3;

    public static final byte GTC = 0;
    public static final byte IOC = 1;
    public static final byte FOK_BUDGET = 2;

    private final int count;
    private final int fillCount;
    private final byte[] types;
    private final byte[] orderTypes;
    private final boolean[] asks;
    private final long[] orderIds;
    private final int[] uids;
    private final long[] prices;
    private final long[] sizes;
    private final long[] reservePrices;

    private final long counterPlaceLimit;
    private final long counterPlaceMarket;
    private final long counterCancel;
    private final long counterMove;
    private final long counterReduce;

    Workload(
            final int count,
            final int fillCount,
            final byte[] types,
            final byte[] orderTypes,
            final boolean[] asks,
            final long[] orderIds,
            final int[] uids,
            final long[] prices,
            final long[] sizes,
            final long[] reservePrices,
            final long counterPlaceLimit,
            final long counterPlaceMarket,
            final long counterCancel,
            final long counterMove,
            final long counterReduce) {
        this.count = count;
        this.fillCount = fillCount;
        this.types = types;
        this.orderTypes = orderTypes;
        this.asks = asks;
        this.orderIds = orderIds;
        this.uids = uids;
        this.prices = prices;
        this.sizes = sizes;
        this.reservePrices = reservePrices;
        this.counterPlaceLimit = counterPlaceLimit;
        this.counterPlaceMarket = counterPlaceMarket;
        this.counterCancel = counterCancel;
        this.counterMove = counterMove;
        this.counterReduce = counterReduce;
    }

    public int count() {
        return count;
    }

    public int fillCount() {
        return fillCount;
    }

    public byte type(final int i) {
        return types[i];
    }

    public byte orderType(final int i) {
        return orderTypes[i];
    }

    public boolean ask(final int i) {
        return asks[i];
    }

    public long orderId(final int i) {
        return orderIds[i];
    }

    public int uid(final int i) {
        return uids[i];
    }

    /** PLACE price, MOVE new price, or FOK-BUDGET budget. */
    public long price(final int i) {
        return prices[i];
    }

    /** PLACE size, or REDUCE reduce-by size. */
    public long size(final int i) {
        return sizes[i];
    }

    public long reservePrice(final int i) {
        return reservePrices[i];
    }

    public long counterPlaceLimit() {
        return counterPlaceLimit;
    }

    public long counterPlaceMarket() {
        return counterPlaceMarket;
    }

    public long counterCancel() {
        return counterCancel;
    }

    public long counterMove() {
        return counterMove;
    }

    public long counterReduce() {
        return counterReduce;
    }
}
