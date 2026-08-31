package io.justrade.bench;

import java.util.ArrayDeque;
import java.util.ArrayList;
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
 * level per symbol means pure FIFO per side, every order has size 1 (so fills
 * are all-or-nothing), and asks reserve base while bids reserve quote at the
 * fill price. Fees are zero (every symbol is registered with zero fees). Every
 * decided command succeeds: cancels and reduces only ever target orders the
 * simulation still holds as resting, and a cancel or reduce with an empty
 * target side falls back to placing a maker ask.
 *
 * <p>Multi-symbol mode shards the round-robin across {@code symbols} symbols,
 * each with its own resting book and price. All symbols share the same base /
 * quote currency pair, so per-user balances and conservation totals are
 * unchanged from the single-symbol shape.
 *
 * <p>Ledger bounds: keep {@code places(uid) < maxOrdersPerUser} and
 * {@code fills(uid) < tradeLimit} per user or the read replica's per-user order
 * history and trade tape evict old records and count assertions fail. The
 * default 100k ops over 100 users stay well inside those limits.
 */
public final class LoadWorkload {

    /** The single symbol exercised by the single-symbol workload. */
    public static final int SYMBOL = 1;
    /** Base currency shared by every symbol. */
    public static final int BASE_CURRENCY = 10;
    /** Quote currency shared by every symbol. */
    public static final int QUOTE_CURRENCY = 20;
    /** The price level of the single-symbol workload (symbol 1). */
    public static final long PRICE = 100L;
    /** Base funding every user receives in setup. */
    public static final long BASE_FUNDING_PER_USER = 1_000_000L;
    /** Quote funding every user receives in setup. */
    public static final long QUOTE_FUNDING_PER_USER = 1_000_000_000L;

    /** The price level at which symbol {@code symbolId} is placed and filled. */
    public static long price(final int symbolId) {
        return PRICE + (symbolId - 1L);
    }

    /** One resting order in the simulated book. */
    public record Resting(int symbolId, long orderId, long uid, boolean ask, long price, long size) {}

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
     * @param symbolId the symbol this command targets
     * @param uid acting user for {@code PLACE}/{@code ORDER_BOOK}, order owner
     *     for {@code CANCEL}/{@code REDUCE}
     * @param orderId fresh id for {@code PLACE}, target id otherwise
     * @param ask ask side for {@code PLACE}
     * @param reserveBidPrice bid reserve for {@code PLACE} (0 for asks)
     */
    public record Command(Type type, int symbolId, long uid, long orderId, boolean ask, long reserveBidPrice) {}

    private final int ops;
    private final int users;
    private final int symbols;

    private final ArrayDeque<Resting>[] bidsBySymbol;
    private final ArrayDeque<Resting>[] asksBySymbol;
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
        this(ops, users, 1);
    }

    /**
     * @param ops number of main-loop iterations (each yields one command)
     * @param users number of users (uid 1..users), cycled round-robin
     * @param symbols number of symbols (symbolId 1..symbols), sharded round-robin
     */
    public LoadWorkload(final int ops, final int users, final int symbols) {
        if (symbols <= 0) {
            throw new IllegalArgumentException("symbols must be positive, was: " + symbols);
        }
        this.ops = ops;
        this.users = users;
        this.symbols = symbols;
        this.bidsBySymbol = newQueues(symbols);
        this.asksBySymbol = newQueues(symbols);
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

    @SuppressWarnings("unchecked")
    private static ArrayDeque<Resting>[] newQueues(final int symbols) {
        final ArrayDeque<Resting>[] queues = (ArrayDeque<Resting>[]) new ArrayDeque<?>[symbols + 1];
        for (int s = 1; s <= symbols; s++) {
            queues[s] = new ArrayDeque<>();
        }
        return queues;
    }

    /**
     * Decides the command for iteration {@code i} and applies it to the
     * simulated book. The caller submits the returned command to the engine;
     * the engine's outcome must match the simulated one.
     */
    public Command next(final int i) {
        final int u = 1 + (i % users);
        final int s = 1 + (i % symbols);
        // Derive the command type from the per-symbol iteration index (i / symbols)
        // rather than the global index (i). Using i % 8 directly would correlate
        // with i % symbols whenever symbols is a multiple of 8, collapsing every
        // symbol to a single command type: no cross-side trading and unbounded
        // resting orders. For symbols == 1 this is identical to the old i % 8.
        switch ((i / symbols) % 8) {
            case 0, 1, 2 -> {
                return placeBid(s, u);
            }
            case 3 -> {
                return placeAsk(s, u);
            }
            case 4 -> {
                final ArrayDeque<Resting> asks = asksBySymbol[s];
                if (!asks.isEmpty()) {
                    return cancelHead(s, asks);
                }
                final ArrayDeque<Resting> bids = bidsBySymbol[s];
                if (!bids.isEmpty()) {
                    return cancelHead(s, bids);
                }
                return placeAsk(s, u);
            }
            case 5 -> {
                if (!asksBySymbol[s].isEmpty()) {
                    return reduceHeadAsk(s);
                }
                return placeAsk(s, u);
            }
            case 6 -> {
                return orderBookRequest(s, u);
            }
            default -> {
                return placeAsk(s, u);
            }
        }
    }

    private Command placeBid(final int symbolId, final int u) {
        final long orderId = orderIdCounter++;
        final long price = price(symbolId);
        places[u]++;
        quoteFree[u] -= price;
        final ArrayDeque<Resting> asks = asksBySymbol[symbolId];
        if (!asks.isEmpty()) {
            final Resting maker = asks.pollFirst();
            fillAgainstAsk(u, maker.uid, price);
        } else {
            bidsBySymbol[symbolId].addLast(new Resting(symbolId, orderId, u, false, price, 1L));
        }
        return new Command(Type.PLACE, symbolId, u, orderId, false, price);
    }

    private Command placeAsk(final int symbolId, final int u) {
        final long orderId = orderIdCounter++;
        final long price = price(symbolId);
        places[u]++;
        baseFree[u] -= 1L;
        final ArrayDeque<Resting> bids = bidsBySymbol[symbolId];
        if (!bids.isEmpty()) {
            final Resting maker = bids.pollFirst();
            fillAgainstBid(u, maker.uid, price);
        } else {
            asksBySymbol[symbolId].addLast(new Resting(symbolId, orderId, u, true, price, 1L));
        }
        return new Command(Type.PLACE, symbolId, u, orderId, true, 0L);
    }

    private Command cancelHead(final int symbolId, final ArrayDeque<Resting> queue) {
        final Resting target = queue.pollFirst();
        cancels++;
        release(target);
        return new Command(Type.CANCEL, symbolId, target.uid, target.orderId, target.ask, 0L);
    }

    private Command reduceHeadAsk(final int symbolId) {
        final Resting target = asksBySymbol[symbolId].pollFirst();
        reduces++;
        release(target);
        return new Command(Type.REDUCE, symbolId, target.uid, target.orderId, true, 0L);
    }

    private Command orderBookRequest(final int symbolId, final int u) {
        orderBookRequests++;
        return new Command(Type.ORDER_BOOK, symbolId, u, 0L, false, 0L);
    }

    private void fillAgainstAsk(final int bidder, final long askerUid, final long price) {
        trades++;
        fills[bidder]++;
        fills[(int) askerUid]++;
        baseFree[bidder] += 1L;
        quoteFree[(int) askerUid] += price;
    }

    private void fillAgainstBid(final int asker, final long bidderUid, final long price) {
        trades++;
        fills[asker]++;
        fills[(int) bidderUid]++;
        baseFree[(int) bidderUid] += 1L;
        quoteFree[asker] += price;
    }

    private void release(final Resting resting) {
        if (resting.ask) {
            baseFree[(int) resting.uid] += 1L;
        } else {
            quoteFree[(int) resting.uid] += resting.price;
        }
    }

    public int ops() {
        return ops;
    }

    public int users() {
        return users;
    }

    public int symbols() {
        return symbols;
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

    /** Expected resting asks of one symbol, oldest first. */
    public List<Resting> restingAsks(final int symbolId) {
        return List.copyOf(asksBySymbol[symbolId]);
    }

    /** Expected resting bids of one symbol, oldest first. */
    public List<Resting> restingBids(final int symbolId) {
        return List.copyOf(bidsBySymbol[symbolId]);
    }

    /** Expected resting asks across all symbols, ascending symbolId then oldest first. */
    public List<Resting> restingAsks() {
        return mergeResting(asksBySymbol);
    }

    /** Expected resting bids across all symbols, ascending symbolId then oldest first. */
    public List<Resting> restingBids() {
        return mergeResting(bidsBySymbol);
    }

    private static List<Resting> mergeResting(final ArrayDeque<Resting>[] bySymbol) {
        final List<Resting> merged = new ArrayList<>();
        for (int s = 1; s < bySymbol.length; s++) {
            merged.addAll(bySymbol[s]);
        }
        return List.copyOf(merged);
    }
}
