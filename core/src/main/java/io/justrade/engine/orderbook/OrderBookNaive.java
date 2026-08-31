package io.justrade.engine.orderbook;

import io.justrade.core.CommandOutcome;
import io.justrade.engine.risk.SymbolSpec;
import io.justrade.protocol.CommandResultCode;
import io.justrade.util.Amounts;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * Deterministic naive order book for one symbol: price-time priority matching
 * with GTC / IOC / FOK-BUDGET semantics, ported from exchange-core's
 * {@code OrderBookNaiveImpl} onto Agrona structures (no TreeMap, no streams).
 *
 * <p>Matcher events (trade / reduce / reject) are accumulated into the supplied
 * {@link CommandOutcome}. Settlement (balances) is applied by risk in phase 3.
 */
public final class OrderBookNaive {

    /**
     * Sentinel returned by {@link #matchFokBudget} instead of a fill count: the
     * ask order was killed before any book mutation because the walked proceeds
     * cannot cover the total taker fee, so a full fill would settle to a
     * negative taker balance.
     */
    public static final long FOK_KILLED_FEE_FLOOR = -2L;

    /**
     * Sentinel returned by {@link #matchFokBudget} instead of a fill count: the
     * ask order was killed before any book mutation because the walked proceeds
     * exceed the signed 64-bit range once quote-scaled, so settlement could
     * never be represented.
     */
    public static final long FOK_KILLED_OVERFLOW = -3L;

    private static final long BUDGET_UNAVAILABLE = -1L;

    /** Walked cost exceeded the signed 64-bit range before the size was covered. */
    private static final long BUDGET_OVERFLOW = -4L;

    private final int symbolId;
    private final OrderNodePool pool;
    private final OrderBookSide askSide;
    private final OrderBookSide bidSide;
    private final Long2ObjectHashMap<OrderNode> idMap = new Long2ObjectHashMap<>();

    public OrderBookNaive(final int symbolId) {
        this(symbolId, new OrderNodePool(1024), new PriceBucketPool(1024));
    }

    public OrderBookNaive(final int symbolId, final OrderNodePool pool) {
        this(symbolId, pool, new PriceBucketPool(1024));
    }

    public OrderBookNaive(final int symbolId, final OrderNodePool pool, final PriceBucketPool bucketPool) {
        this.symbolId = symbolId;
        this.pool = pool;
        this.askSide = new OrderBookSide(true, bucketPool);
        this.bidSide = new OrderBookSide(false, bucketPool);
    }

    /** Places a GTC order: match marketable portion, then rest the remainder. */
    public long placeGtc(
            final long orderId,
            final boolean ask,
            final long price,
            final long size,
            final long reserveBidPrice,
            final long uid,
            final long timestamp,
            final CommandOutcome out) {
        final long filled = matchInstantly(ask, price, size, 0L, uid, reserveBidPrice, true, out);
        if (filled == size) {
            return filled;
        }
        if (idMap.containsKey(orderId)) {
            // Duplicate id: matched what it could, but cannot rest; reject remainder.
            out.addReject(symbolId, orderId, uid, size - filled, price);
            return filled;
        }
        final OrderNode node = pool.acquire();
        node.set(orderId, price, size, filled, reserveBidPrice, uid, timestamp, ask);
        ownSide(ask).getOrCreate(price).append(node);
        idMap.put(orderId, node);
        return filled;
    }

    /** Places an IOC order: match marketable portion, reject the remainder. */
    public long matchIoc(
            final long orderId,
            final boolean ask,
            final long price,
            final long size,
            final long reserveBidPrice,
            final long uid,
            final CommandOutcome out) {
        final long filled = matchInstantly(ask, price, size, 0L, uid, reserveBidPrice, true, out);
        final long rejected = size - filled;
        if (rejected > 0L) {
            out.addReject(symbolId, orderId, uid, rejected, price);
        }
        return filled;
    }

    /**
     * Places a fill-or-kill budget order: fill fully within budget or reject
     * wholesale. For an ask the budget is a MINIMUM-proceeds bound and the walk
     * is price-unlimited, so the walked proceeds - not the budget - are
     * validated against settlement capacity before any book mutation; a kill
     * there returns {@link #FOK_KILLED_FEE_FLOOR} or {@link #FOK_KILLED_OVERFLOW}
     * instead of a fill count.
     */
    public long matchFokBudget(
            final long orderId,
            final boolean ask,
            final long budget,
            final long size,
            final long reserveBidPrice,
            final long uid,
            final SymbolSpec spec,
            final CommandOutcome out) {
        final long cost = budgetToFill(ask, size);
        if (ask && cost == BUDGET_OVERFLOW) {
            // An overflowed walked cost means the settlement proceeds are
            // unrepresentable regardless of the budget.
            out.addReject(symbolId, orderId, uid, size, budget);
            return FOK_KILLED_OVERFLOW;
        }
        if (ask && cost >= 0L) {
            if (Amounts.mulOverflows(cost, spec.quoteScaleK()) || Amounts.mulOverflows(spec.takerFee(), size)) {
                out.addReject(symbolId, orderId, uid, size, budget);
                return FOK_KILLED_OVERFLOW;
            }
            if (cost * spec.quoteScaleK() < spec.takerFee() * size) {
                out.addReject(symbolId, orderId, uid, size, budget);
                return FOK_KILLED_FEE_FLOOR;
            }
        }
        if (cost < 0L || !budgetSatisfied(ask, cost, budget)) {
            out.addReject(symbolId, orderId, uid, size, budget);
            return 0L;
        }
        return matchInstantly(ask, 0L, size, 0L, uid, reserveBidPrice, false, out);
    }

    /** Cancels a resting order; releases its whole remaining size via a reduce event. */
    public CommandResultCode cancel(final long orderId, final long uid, final CommandOutcome out) {
        final OrderNode node = idMap.get(orderId);
        if (node == null || node.uid != uid) {
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }
        idMap.remove(orderId);
        final OrderBookSide side = ownSide(node.ask);
        final PriceBucket bucket = side.find(node.price);
        final long reducedBy = node.remaining();
        bucket.removeOrder(node);
        if (bucket.numOrders == 0) {
            side.remove(bucket);
        }
        out.addReduce(symbolId, orderId, uid, reducedBy, !node.ask, node.reserveBidPrice, node.price, true);
        pool.release(node);
        return CommandResultCode.SUCCESS;
    }

    /** Reduces a resting order's size; removes it when reduced to nothing. */
    public CommandResultCode reduce(
            final long orderId, final long uid, final long requestedSize, final CommandOutcome out) {
        if (requestedSize <= 0L) {
            return CommandResultCode.MATCHING_REDUCE_FAILED_WRONG_SIZE;
        }
        final OrderNode node = idMap.get(orderId);
        if (node == null || node.uid != uid) {
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }
        final long remaining = node.remaining();
        final long reduceBy = Math.min(remaining, requestedSize);
        final OrderBookSide side = ownSide(node.ask);
        final PriceBucket bucket = side.find(node.price);
        if (reduceBy == remaining) {
            idMap.remove(orderId);
            bucket.removeOrder(node);
            if (bucket.numOrders == 0) {
                side.remove(bucket);
            }
            out.addReduce(symbolId, orderId, uid, reduceBy, !node.ask, node.reserveBidPrice, node.price, true);
            pool.release(node);
            return CommandResultCode.SUCCESS;
        }
        node.size -= reduceBy;
        bucket.totalVolume -= reduceBy;
        out.addReduce(symbolId, orderId, uid, reduceBy, !node.ask, node.reserveBidPrice, node.price, false);
        return CommandResultCode.SUCCESS;
    }

    /** Moves a resting order to a new price; may become marketable and match immediately. */
    public CommandResultCode move(
            final long orderId, final long uid, final long newPrice, final SymbolSpec spec, final CommandOutcome out) {
        final OrderNode node = idMap.get(orderId);
        if (node == null || node.uid != uid) {
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }
        if (newPrice <= 0L) {
            return CommandResultCode.INVALID_AMOUNT;
        }
        // A bid cannot move above its reserved price: the hold would be insufficient.
        if (!node.ask && newPrice > node.reserveBidPrice) {
            return CommandResultCode.MATCHING_MOVE_FAILED_PRICE_OVER_RISK_LIMIT;
        }
        if (node.ask) {
            // The same fee floor as PLACE: a lower price would settle to negative
            // proceeds. Overflow-safe: an overflowing product exceeds any fitting fee.
            if (!Amounts.mulOverflows(newPrice, spec.quoteScaleK())
                    && newPrice * spec.quoteScaleK() < spec.takerFee()) {
                return CommandResultCode.RISK_ASK_PRICE_LOWER_THAN_FEE;
            }
            // Bound the credits a fill at the new price can produce before the book
            // is mutated, mirroring the PLACE pre-checks for a selling order.
            final long remaining = node.remaining();
            if (Amounts.mulOverflows(remaining, newPrice)
                    || Amounts.mulOverflows(remaining * newPrice, spec.quoteScaleK())
                    || Amounts.mulOverflows(spec.takerFee(), remaining)) {
                return CommandResultCode.OVERFLOW;
            }
        }
        final OrderBookSide side = ownSide(node.ask);
        final PriceBucket bucket = side.find(node.price);
        bucket.removeOrder(node);
        if (bucket.numOrders == 0) {
            side.remove(bucket);
        }
        node.price = newPrice;
        final long filled =
                matchInstantly(node.ask, newPrice, node.size, node.filled, node.uid, node.reserveBidPrice, true, out);
        if (filled == node.size) {
            idMap.remove(orderId);
            pool.release(node);
            return CommandResultCode.SUCCESS;
        }
        node.filled = filled;
        side.getOrCreate(newPrice).append(node);
        return CommandResultCode.SUCCESS;
    }

    /** Builds an L2 snapshot into {@code view}, bounded by {@code view.maxLevels()}. */
    public void fillL2(final L2View view) {
        view.reset();
        final int maxLevels = view.maxLevels();
        PriceBucket bucket = askSide.best();
        for (int i = 0; bucket != null && i < maxLevels; i++) {
            view.addAsk(bucket.price, bucket.totalVolume, bucket.numOrders);
            bucket = bucket.worse;
        }
        bucket = bidSide.best();
        for (int i = 0; bucket != null && i < maxLevels; i++) {
            view.addBid(bucket.price, bucket.totalVolume, bucket.numOrders);
            bucket = bucket.worse;
        }
    }

    private long matchInstantly(
            final boolean takerAsk,
            final long limitPrice,
            final long size,
            final long alreadyFilled,
            final long takerUid,
            final long takerReserveBidPrice,
            final boolean priceLimited,
            final CommandOutcome out) {
        final OrderBookSide opposite = takerAsk ? bidSide : askSide;
        long filled = alreadyFilled;
        PriceBucket bucket = opposite.best();
        while (bucket != null && filled < size) {
            if (priceLimited && !opposite.marketable(bucket.price, limitPrice)) {
                break;
            }
            final PriceBucket nextBucket = bucket.worse;
            OrderNode node = bucket.firstOrder;
            while (node != null && filled < size) {
                final OrderNode nextNode = node.bucketNext;
                final long fillVolume = Math.min(size - filled, node.remaining());
                node.filled += fillVolume;
                filled += fillVolume;
                bucket.totalVolume -= fillVolume;
                final boolean makerCompleted = node.remaining() == 0L;
                out.addTrade(
                        symbolId,
                        node.orderId,
                        node.uid,
                        takerUid,
                        bucket.price,
                        fillVolume,
                        makerCompleted,
                        !node.ask,
                        node.reserveBidPrice,
                        !takerAsk,
                        takerReserveBidPrice);
                if (makerCompleted) {
                    bucket.removeOrder(node);
                    idMap.remove(node.orderId);
                    pool.release(node);
                }
                node = nextNode;
            }
            if (bucket.numOrders == 0) {
                opposite.remove(bucket);
            }
            bucket = nextBucket;
        }
        return filled;
    }

    private long budgetToFill(final boolean takerAsk, final long size) {
        final OrderBookSide opposite = takerAsk ? bidSide : askSide;
        long remaining = size;
        long budget = 0L;
        PriceBucket bucket = opposite.best();
        while (bucket != null) {
            final long available = bucket.totalVolume;
            if (remaining > available) {
                remaining -= available;
                // Checked accumulation: for a bid, a book deep enough to overflow
                // the walked cost cannot satisfy any fitting budget, so plain
                // unavailability suffices; for an ask the overflowed cost means
                // settlement itself is unrepresentable, so keep the two causes
                // distinct.
                if (Amounts.mulOverflows(available, bucket.price)
                        || Amounts.addOverflows(budget, available * bucket.price)) {
                    return BUDGET_OVERFLOW;
                }
                budget += available * bucket.price;
                bucket = bucket.worse;
            } else {
                if (Amounts.mulOverflows(remaining, bucket.price)
                        || Amounts.addOverflows(budget, remaining * bucket.price)) {
                    return BUDGET_OVERFLOW;
                }
                return budget + remaining * bucket.price;
            }
        }
        return BUDGET_UNAVAILABLE;
    }

    private boolean budgetSatisfied(final boolean takerAsk, final long cost, final long budget) {
        // Seller wants proceeds at least the budget; buyer wants cost at most the budget.
        return takerAsk ? cost >= budget : cost <= budget;
    }

    private OrderBookSide ownSide(final boolean ask) {
        return ask ? askSide : bidSide;
    }

    /**
     * Directly inserts a resting order without matching; used only to restore a
     * book from a snapshot. Orders must be restored in book order (best-first,
     * FIFO within a price) so the reconstructed structure is identical.
     */
    public void restingInsert(
            final long orderId,
            final boolean ask,
            final long price,
            final long size,
            final long filled,
            final long reserveBidPrice,
            final long uid,
            final long timestamp) {
        final OrderNode node = pool.acquire();
        node.set(orderId, price, size, filled, reserveBidPrice, uid, timestamp, ask);
        ownSide(ask).getOrCreate(price).append(node);
        idMap.put(orderId, node);
    }

    /**
     * Emits every resting order in deterministic book order: ask side best-to-worse
     * then bid side best-to-worse, FIFO within each price level.
     *
     * <p>Cold snapshot path only.
     */
    public void forEachOrderSorted(final OrderConsumer consumer) {
        emitSide(askSide, consumer);
        emitSide(bidSide, consumer);
    }

    private void emitSide(final OrderBookSide side, final OrderConsumer consumer) {
        PriceBucket bucket = side.best();
        while (bucket != null) {
            OrderNode node = bucket.firstOrder;
            while (node != null) {
                consumer.accept(
                        node.orderId,
                        node.ask,
                        node.price,
                        node.size,
                        node.filled,
                        node.reserveBidPrice,
                        node.uid,
                        node.timestamp);
                node = node.bucketNext;
            }
            bucket = bucket.worse;
        }
    }

    /** Callback for deterministic resting-order iteration. */
    @FunctionalInterface
    public interface OrderConsumer {
        void accept(
                long orderId,
                boolean ask,
                long price,
                long size,
                long filled,
                long reserveBidPrice,
                long uid,
                long timestamp);
    }

    public boolean contains(final long orderId) {
        return idMap.containsKey(orderId);
    }

    public int orderCount() {
        return idMap.size();
    }

    public int askBucketCount() {
        return askSide.bucketCount();
    }

    public int bidBucketCount() {
        return bidSide.bucketCount();
    }

    /** Best ask price, or {@link Long#MAX_VALUE} when the ask side is empty. */
    public long bestAsk() {
        final PriceBucket best = askSide.best();
        return best == null ? Long.MAX_VALUE : best.price;
    }

    /** Best bid price, or {@link Long#MIN_VALUE} when the bid side is empty. */
    public long bestBid() {
        final PriceBucket best = bidSide.best();
        return best == null ? Long.MIN_VALUE : best.price;
    }
}
