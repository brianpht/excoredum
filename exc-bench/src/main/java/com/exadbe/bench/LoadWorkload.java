package com.exadbe.bench;

import java.util.ArrayDeque;
import java.util.List;

/**
 * Deterministic synthetic workload for end-to-end system tests, plus an exact
 * simulation of the matching engine's single-price-level book. The write-side
 * load runner submits the commands this workload decides and the read-side
 * verifier replays the same simulation, so both sides agree on the expected
 * final state (balances, resting orders, trade and history counts) without
 * exchanging data.
 *
 * <p>The simulation models the engine's price-time priority exactly: one price
 * level means pure FIFO per side, every order has size 1 (so fills are
 * all-or-nothing), and asks reserve base while bids reserve quote at the
 * fill price. Fees are zero (the symbol is registered with zero fees). Every
 * decided command succeeds: cancels and reduces only ever target orders the
 * simulation still holds as resting, and a cancel or reduce with an empty
 * target side falls back to placing a maker ask.
 *
 * <p>Ledger bounds: keep {@code places(uid) < 4096} and {@code fills(uid) <
 * 4096} per user or the read replica's per-user order history and trade tape
 * evict old records and count assertions fail. The default 100k ops over 100
 * users stay well inside those limits.
 */
public final class LoadWorkload {

    /** The single symbol exercised by the workload. */
    public static final int SYMBOL = 1;
    /** Base currency of {@link #SYMBOL}. */
    public static final int BASE_CURRENCY = 10;
    /** Quote currency of {@link #SYMBOL}. */
    public static final int QUOTE_CURRENCY = 20;
    /** The only price level at which every order is placed and filled. */
    public static final long PRICE = 100L;
    /** Base funding every user receives in setup. */
    public static final long BASE_FUNDING_PER_USER = 1_000_000L;
    /** Quote funding every user receives in setup. */
    public static final long QUOTE_FUNDING_PER_USER = 1_000_000_000L;

    /** One resting order in the simulated book. */
    public record Resting(long orderId, long uid, boolean ask, long price, long size) {}

    /** The concrete command the workload decides for one iteration. */
    public enum Type {
        PLACE,
        CANCEL,
        REDUCE,
        ORDER_BOOK
    }

    /**
     * @param type the command kind; {@code PLACE} carries the acting user,
     *     {@code CANCEL}/{@code REDUCE} carry the resting order's owner and id
     * @param uid acting user for {@code PLACE}/{@code ORDER_BOOK}, order owner
     *     for {@code CANCEL}/{@code REDUCE}
     * @param orderId fresh id for {@code PLACE}, target id otherwise
     * @param ask ask side for {@code PLACE}
     * @param reserveBidPrice bid reserve for {@code PLACE} (0 for asks)
     */
    public record Command(Type type, long uid, long orderId, boolean ask, long reserveBidPrice) {}

    private final int ops;
    private final int users;

    private final ArrayDeque<Resting> bids = new ArrayDeque<>();
    private final ArrayDeque<Resting> asks = new ArrayDeque<>();
    private final long[] baseFree;
    private final long[] quoteFree;
    private final int[] places;
    private final int[] fills;
    private long orderIdCounter = 1L;
    private long trades;
    private int cancels;
    private int reduces;
    private int orderBookRequests;

    /**
     * @param ops number of main-loop iterations (each yields one command)
     * @param users number of users (uid 1..users), cycled round-robin
     */
    public LoadWorkload(final int ops, final int users) {
        this.ops = ops;
        this.users = users;
        this.baseFree = new long[users + 1];
        this.quoteFree = new long[users + 1];
        this.places = new int[users + 1];
        this.fills = new int[users + 1];
        // The engine starts every user funded (the load runner submits one
        // balance adjustment per currency in setup), so expected free balances
        // are funding plus the simulated reserve / settle / release deltas.
        for (int u = 1; u <= users; u++) {
            baseFree[u] = BASE_FUNDING_PER_USER;
            quoteFree[u] = QUOTE_FUNDING_PER_USER;
        }
    }

    /**
     * Decides the command for iteration {@code i} and applies it to the
     * simulated book. The caller submits the returned command to the engine;
     * the engine's outcome must match the simulated one.
     */
    public Command next(final int i) {
        final int u = 1 + (i % users);
        switch (i % 8) {
            case 0, 1, 2 -> {
                return placeBid(u);
            }
            case 3 -> {
                return placeAsk(u);
            }
            case 4 -> {
                if (!asks.isEmpty()) {
                    return cancelHead(asks);
                }
                if (!bids.isEmpty()) {
                    return cancelHead(bids);
                }
                return placeAsk(u);
            }
            case 5 -> {
                if (!asks.isEmpty()) {
                    return reduceHeadAsk();
                }
                return placeAsk(u);
            }
            case 6 -> {
                return orderBookRequest(u);
            }
            default -> {
                return placeAsk(u);
            }
        }
    }

    private Command placeBid(final int u) {
        final long orderId = orderIdCounter++;
        places[u]++;
        quoteFree[u] -= PRICE;
        if (!asks.isEmpty()) {
            final Resting maker = asks.pollFirst();
            fillAgainstAsk(u, maker.uid);
        } else {
            bids.addLast(new Resting(orderId, u, false, PRICE, 1L));
        }
        return new Command(Type.PLACE, u, orderId, false, PRICE);
    }

    private Command placeAsk(final int u) {
        final long orderId = orderIdCounter++;
        places[u]++;
        baseFree[u] -= 1L;
        if (!bids.isEmpty()) {
            final Resting maker = bids.pollFirst();
            fillAgainstBid(u, maker.uid);
        } else {
            asks.addLast(new Resting(orderId, u, true, PRICE, 1L));
        }
        return new Command(Type.PLACE, u, orderId, true, 0L);
    }

    private Command cancelHead(final ArrayDeque<Resting> queue) {
        final Resting target = queue.pollFirst();
        cancels++;
        release(target);
        return new Command(Type.CANCEL, target.uid, target.orderId, target.ask, 0L);
    }

    private Command reduceHeadAsk() {
        final Resting target = asks.pollFirst();
        reduces++;
        release(target);
        return new Command(Type.REDUCE, target.uid, target.orderId, true, 0L);
    }

    private Command orderBookRequest(final int u) {
        orderBookRequests++;
        return new Command(Type.ORDER_BOOK, u, 0L, false, 0L);
    }

    private void fillAgainstAsk(final int bidder, final long askerUid) {
        trades++;
        fills[bidder]++;
        fills[(int) askerUid]++;
        baseFree[bidder] += 1L;
        quoteFree[(int) askerUid] += PRICE;
    }

    private void fillAgainstBid(final int asker, final long bidderUid) {
        trades++;
        fills[asker]++;
        fills[(int) bidderUid]++;
        baseFree[(int) bidderUid] += 1L;
        quoteFree[asker] += PRICE;
    }

    private void release(final Resting resting) {
        if (resting.ask) {
            baseFree[(int) resting.uid] += 1L;
        } else {
            quoteFree[(int) resting.uid] += PRICE;
        }
    }

    public int ops() {
        return ops;
    }

    public int users() {
        return users;
    }

    /** Expected total fills (trades) after the full workload. */
    public long trades() {
        return trades;
    }

    public int cancels() {
        return cancels;
    }

    public int reduces() {
        return reduces;
    }

    public int orderBookRequests() {
        return orderBookRequests;
    }

    /** Expected free base balance of {@code uid} after the full workload. */
    public long baseFree(final long uid) {
        return baseFree[(int) uid];
    }

    /** Expected free quote balance of {@code uid} after the full workload. */
    public long quoteFree(final long uid) {
        return quoteFree[(int) uid];
    }

    /** Expected place commands submitted by {@code uid}. */
    public int places(final long uid) {
        return places[(int) uid];
    }

    /** Expected fills involving {@code uid} after the full workload. */
    public int fills(final long uid) {
        return fills[(int) uid];
    }

    /** Expected total place commands across all users after the full workload. */
    public long totalPlaces() {
        long total = 0L;
        for (int u = 1; u <= users; u++) {
            total += places[u];
        }
        return total;
    }

    /** Expected resting asks after the full workload, oldest first. */
    public List<Resting> restingAsks() {
        return List.copyOf(asks);
    }

    /** Expected resting bids after the full workload, oldest first. */
    public List<Resting> restingBids() {
        return List.copyOf(bids);
    }
}
