package io.justrade.xcorebench;

import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.MatcherEventType;
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.SymbolType;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.LoggingConfiguration;
import exchange.core2.core.orderbook.IOrderBook;
import exchange.core2.core.orderbook.OrderBookNaiveImpl;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;

/**
 * Deterministic workload generator: a faithful port of exchange-core's
 * {@code TestOrdersGenerator} (src/test of exchange-core 0.5.3, Apache-2.0,
 * Copyright 2019 Maksim Zheravin), adapted to emit {@link Workload} primitive
 * arrays instead of {@code OrderCommand}/{@code ApiCommand} lists.
 *
 * <p>As in the original, generation replays every command against an internal
 * {@link OrderBookNaiveImpl} so the emitted sequence is legal and its
 * fill/reject feedback drives the mix; the seed fully determines the output.
 * The generation book uses a spot {@code CURRENCY_EXCHANGE_PAIR} symbol so the
 * sequence is also valid for justrade's spot-only engine.
 */
public final class WorkloadGenerator {

    /** Same per-command order-book stat refresh interval as the original. */
    public static final int CHECK_ORDERBOOK_STAT_EVERY_NTH_COMMAND = 512;

    private static final double CENTRAL_MOVE_ALPHA = 0.01;

    private WorkloadGenerator() {}

    /** Generates one fill phase plus one benchmark phase for a single symbol. */
    public static Workload generate(
            final int benchmarkTransactionsNumber,
            final int targetOrderBookOrders,
            final int numUsers,
            final int symbolId,
            final boolean enableSlidingPrice,
            final boolean avalancheIoc,
            final int seed) {

        final CoreSymbolSpecification symbolSpec = spotSymbol(symbolId);
        final Session session = new Session(
                new OrderBookNaiveImpl(symbolSpec, LoggingConfiguration.DEFAULT),
                benchmarkTransactionsNumber,
                targetOrderBookOrders / 2,
                avalancheIoc,
                numUsers,
                symbolId,
                enableSlidingPrice,
                seed);

        final int total = benchmarkTransactionsNumber + targetOrderBookOrders;
        final byte[] types = new byte[total];
        final byte[] orderTypes = new byte[total];
        final boolean[] asks = new boolean[total];
        final long[] orderIds = new long[total];
        final int[] uids = new int[total];
        final long[] prices = new long[total];
        final long[] sizes = new long[total];
        final long[] reservePrices = new long[total];

        int nextSizeCheck = Math.min(CHECK_ORDERBOOK_STAT_EVERY_NTH_COMMAND, targetOrderBookOrders + 1);

        for (int i = 0; i < total; i++) {
            final OrderCommand cmd =
                    (i < targetOrderBookOrders) ? generateRandomGtcOrder(session) : generateRandomOrder(session);

            cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
            cmd.symbol = session.symbol;

            final CommandResultCode resultCode = IOrderBook.processCommand(session.orderBook, cmd);
            if (resultCode != CommandResultCode.SUCCESS) {
                throw new IllegalStateException("Unsuccessful result code: " + resultCode + " for " + cmd);
            }

            handleEvents(session, cmd);
            cmd.matcherEvent = null;

            types[i] = toType(cmd.command);
            orderTypes[i] = (cmd.command == OrderCommandType.PLACE_ORDER) ? toOrderType(cmd.orderType) : Workload.GTC;
            asks[i] = cmd.action == OrderAction.ASK;
            orderIds[i] = cmd.orderId;
            uids[i] = (int) cmd.uid;
            prices[i] = cmd.price;
            sizes[i] = cmd.size;
            reservePrices[i] = cmd.reserveBidPrice;

            if (i >= nextSizeCheck) {
                nextSizeCheck += Math.min(CHECK_ORDERBOOK_STAT_EVERY_NTH_COMMAND, targetOrderBookOrders + 1);
                updateOrderBookSizeStat(session);
            }
        }

        updateOrderBookSizeStat(session);

        return new Workload(
                total,
                targetOrderBookOrders,
                types,
                orderTypes,
                asks,
                orderIds,
                uids,
                prices,
                sizes,
                reservePrices,
                session.counterPlaceLimit,
                session.counterPlaceMarket,
                session.counterCancel,
                session.counterMove,
                session.counterReduce);
    }

    /** The spot symbol spec shared by the generation book and both bench targets. */
    public static CoreSymbolSpecification spotSymbol(final int symbolId) {
        return CoreSymbolSpecification.builder()
                .symbolId(symbolId)
                .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
                .baseCurrency(11)
                .quoteCurrency(15)
                .baseScaleK(1L)
                .quoteScaleK(1L)
                .takerFee(0L)
                .makerFee(0L)
                .build();
    }

    private static byte toType(final OrderCommandType command) {
        if (command == OrderCommandType.PLACE_ORDER) {
            return Workload.PLACE;
        }
        if (command == OrderCommandType.CANCEL_ORDER) {
            return Workload.CANCEL;
        }
        if (command == OrderCommandType.MOVE_ORDER) {
            return Workload.MOVE;
        }
        if (command == OrderCommandType.REDUCE_ORDER) {
            return Workload.REDUCE;
        }
        throw new IllegalStateException("unexpected command type: " + command);
    }

    private static byte toOrderType(final OrderType orderType) {
        if (orderType == OrderType.GTC) {
            return Workload.GTC;
        }
        if (orderType == OrderType.IOC) {
            return Workload.IOC;
        }
        if (orderType == OrderType.FOK_BUDGET) {
            return Workload.FOK_BUDGET;
        }
        throw new IllegalStateException("unexpected order type: " + orderType);
    }

    private static void handleEvents(final Session session, final OrderCommand cmd) {
        MatcherTradeEvent ev = cmd.matcherEvent;
        while (ev != null) {
            onEvent(session, ev, cmd);
            ev = ev.nextEvent;
        }
    }

    private static void onEvent(final Session session, final MatcherTradeEvent ev, final OrderCommand cmd) {
        final int activeOrderId = (int) cmd.orderId;
        if (ev.eventType == MatcherEventType.TRADE) {
            if (ev.activeOrderCompleted) {
                session.numCompleted++;
            }
            if (ev.matchedOrderCompleted) {
                session.orderUids.remove((int) ev.matchedOrderId);
                session.numCompleted++;
            }
            if (session.orderSizes.addToValue((int) ev.matchedOrderId, (int) -ev.size) < 0) {
                throw new IllegalStateException("Incorrect filled size for order " + ev.matchedOrderId);
            }
            session.lastTradePrice = Math.min(session.maxPrice, Math.max(session.minPrice, ev.price));
            if (ev.price <= session.minPrice) {
                session.priceDirection = 1;
            } else if (ev.price >= session.maxPrice) {
                session.priceDirection = -1;
            }
        } else if (ev.eventType == MatcherEventType.REJECT) {
            session.numRejected++;
            updateOrderBookSizeStat(session);
        } else if (ev.eventType == MatcherEventType.REDUCE) {
            session.numReduced++;
        } else {
            return;
        }

        if (session.orderSizes.addToValue(activeOrderId, (int) -ev.size) < 0) {
            throw new IllegalStateException("Incorrect filled size for order " + activeOrderId);
        }
        if (ev.activeOrderCompleted) {
            session.orderUids.remove(activeOrderId);
        }
    }

    private static void updateOrderBookSizeStat(final Session session) {
        session.lastOrderBookOrdersSizeAsk = session.orderBook.getOrdersNum(OrderAction.ASK);
        session.lastOrderBookOrdersSizeBid = session.orderBook.getOrdersNum(OrderAction.BID);
        if (session.initialOrdersPlaced || session.avalancheIoc) {
            final var l2 = session.orderBook.getL2MarketDataSnapshot(Integer.MAX_VALUE);
            if (session.avalancheIoc) {
                session.lastTotalVolumeAsk = l2.totalOrderBookVolumeAsk();
                session.lastTotalVolumeBid = l2.totalOrderBookVolumeBid();
            }
        }
    }

    private static OrderCommand generateRandomOrder(final Session session) {
        final Random rand = session.rand;

        final int lackOfOrdersAsk = session.targetOrderBookOrdersHalf - session.lastOrderBookOrdersSizeAsk;
        final int lackOfOrdersBid = session.targetOrderBookOrdersHalf - session.lastOrderBookOrdersSizeBid;
        if (!session.initialOrdersPlaced && lackOfOrdersAsk <= 0 && lackOfOrdersBid <= 0) {
            session.initialOrdersPlaced = true;
            session.counterPlaceMarket = 0;
            session.counterPlaceLimit = 0;
            session.counterCancel = 0;
            session.counterMove = 0;
            session.counterReduce = 0;
        }

        final OrderAction action = (rand.nextInt(4) + session.priceDirection >= 2) ? OrderAction.BID : OrderAction.ASK;
        final int lackOfOrders = (action == OrderAction.ASK) ? lackOfOrdersAsk : lackOfOrdersBid;
        final boolean requireFastFill = session.filledAtSeq < 0 || lackOfOrders > session.lackOrOrdersFastFillThreshold;
        final boolean growOrders = lackOfOrders > 0;

        if (session.filledAtSeq < 0 && !growOrders) {
            session.filledAtSeq = session.seq;
        }

        final int q = rand.nextInt(growOrders ? (requireFastFill ? 2 : 10) : 40);

        if (q < 2 || session.orderUids.isEmpty()) {
            if (growOrders) {
                return generateRandomGtcOrder(session);
            }
            return generateRandomInstantOrder(session);
        }

        final int pickBound = Math.min(session.orderUids.size(), 512);
        final int randPos = rand.nextInt(pickBound);
        final Iterator<Map.Entry<Integer, Integer>> iterator =
                session.orderUids.entrySet().iterator();
        Map.Entry<Integer, Integer> rec = iterator.next();
        for (int i = 0; i < randPos; i++) {
            rec = iterator.next();
        }
        final int orderId = rec.getKey();
        final int uid = rec.getValue();
        if (uid == 0) {
            throw new IllegalStateException();
        }

        if (q == 2) {
            session.orderUids.remove(orderId);
            session.counterCancel++;
            return OrderCommand.cancel(orderId, uid);
        }

        if (q == 3) {
            session.counterReduce++;
            final int prevSize = session.orderSizes.get(orderId);
            final int reduceBy = session.rand.nextInt(prevSize) + 1;
            return OrderCommand.reduce(orderId, uid, reduceBy);
        }

        final int prevPrice = session.orderPrices.get(orderId);
        if (prevPrice == 0) {
            throw new IllegalStateException();
        }
        final double priceMove = (session.lastTradePrice - prevPrice) * CENTRAL_MOVE_ALPHA;
        final int priceMoveRounded;
        if (prevPrice > session.lastTradePrice) {
            priceMoveRounded = (int) Math.floor(priceMove);
        } else if (prevPrice < session.lastTradePrice) {
            priceMoveRounded = (int) Math.ceil(priceMove);
        } else {
            priceMoveRounded = rand.nextInt(2) * 2 - 1;
        }
        final int newPrice = Math.min(prevPrice + priceMoveRounded, (int) session.maxPrice);

        session.counterMove++;
        session.orderPrices.put(orderId, newPrice);
        return OrderCommand.update(orderId, uid, newPrice);
    }

    private static OrderCommand generateRandomGtcOrder(final Session session) {
        final Random rand = session.rand;

        final OrderAction action = (rand.nextInt(4) + session.priceDirection >= 2) ? OrderAction.BID : OrderAction.ASK;
        final int uid = 1 + rand.nextInt(session.numUsers);
        final int newOrderId = session.seq;

        final int dev = 1 + (int) (Math.pow(rand.nextDouble(), 2) * session.priceDeviation);
        long p = 0;
        final int x = 4;
        for (int i = 0; i < x; i++) {
            p += rand.nextInt(dev);
        }
        p = p / x * 2 - dev;
        if (p > 0 ^ action == OrderAction.ASK) {
            p = -p;
        }
        final int price = (int) session.lastTradePrice + (int) p;

        final int size = 1 + rand.nextInt(6) * rand.nextInt(6) * rand.nextInt(6);

        session.orderPrices.put(newOrderId, price);
        session.orderSizes.put(newOrderId, size);
        session.orderUids.put(newOrderId, uid);
        session.counterPlaceLimit++;
        session.seq++;

        return OrderCommand.builder()
                .command(OrderCommandType.PLACE_ORDER)
                .uid(uid)
                .orderId(newOrderId)
                .action(action)
                .orderType(OrderType.GTC)
                .size(size)
                .price(price)
                .reserveBidPrice(action == OrderAction.BID ? session.maxPrice : 0)
                .build();
    }

    private static OrderCommand generateRandomInstantOrder(final Session session) {
        final Random rand = session.rand;

        final OrderAction action = (rand.nextInt(4) + session.priceDirection >= 2) ? OrderAction.BID : OrderAction.ASK;
        final int uid = 1 + rand.nextInt(session.numUsers);
        final int newOrderId = session.seq;

        final long priceLimit = action == OrderAction.BID ? session.maxPrice : session.minPrice;

        final long size;
        final OrderType orderType;
        final long priceOrBudget;
        final long reserveBidPrice;

        if (session.avalancheIoc) {
            orderType = OrderType.IOC;
            priceOrBudget = priceLimit;
            reserveBidPrice = action == OrderAction.BID ? session.maxPrice : 0;
            final long availableVolume =
                    action == OrderAction.ASK ? session.lastTotalVolumeAsk : session.lastTotalVolumeBid;
            long bigRand = rand.nextLong();
            bigRand = bigRand < 0 ? -1 - bigRand : bigRand;
            size = 1 + bigRand % (availableVolume + 1);
            if (action == OrderAction.ASK) {
                session.lastTotalVolumeAsk = Math.max(session.lastTotalVolumeAsk - size, 0);
            } else {
                session.lastTotalVolumeBid = Math.max(session.lastTotalVolumeBid - size, 0);
            }
        } else if (rand.nextInt(32) == 0) {
            // IOC : FOK-BUDGET = 31 : 1
            orderType = OrderType.FOK_BUDGET;
            size = 1 + rand.nextInt(8) * rand.nextInt(8) * rand.nextInt(8);
            priceOrBudget = size * priceLimit;
            reserveBidPrice = priceOrBudget;
        } else {
            orderType = OrderType.IOC;
            priceOrBudget = priceLimit;
            reserveBidPrice = action == OrderAction.BID ? session.maxPrice : 0;
            size = 1 + rand.nextInt(6) * rand.nextInt(6) * rand.nextInt(6);
        }

        session.orderSizes.put(newOrderId, (int) size);
        session.counterPlaceMarket++;
        session.seq++;

        return OrderCommand.builder()
                .command(OrderCommandType.PLACE_ORDER)
                .orderType(orderType)
                .uid(uid)
                .orderId(newOrderId)
                .action(action)
                .size(size)
                .price(priceOrBudget)
                .reserveBidPrice(reserveBidPrice)
                .build();
    }

    /** Mutable generation state; mirrors exchange-core's TestOrdersGeneratorSession. */
    private static final class Session {
        final IOrderBook orderBook;
        final int targetOrderBookOrdersHalf;
        final long priceDeviation;
        final boolean avalancheIoc;
        final int numUsers;
        final int symbol;
        final Random rand;

        final IntIntHashMap orderPrices = new IntIntHashMap();
        final IntIntHashMap orderSizes = new IntIntHashMap();
        final LinkedHashMap<Integer, Integer> orderUids = new LinkedHashMap<>();

        final long minPrice;
        final long maxPrice;
        final int lackOrOrdersFastFillThreshold;

        long lastTradePrice;
        int priceDirection;
        boolean initialOrdersPlaced;

        long numCompleted;
        long numRejected;
        long numReduced;

        long counterPlaceMarket;
        long counterPlaceLimit;
        long counterCancel;
        long counterMove;
        long counterReduce;

        int seq = 1;
        int filledAtSeq = -1;

        int lastOrderBookOrdersSizeAsk;
        int lastOrderBookOrdersSizeBid;
        long lastTotalVolumeAsk;
        long lastTotalVolumeBid;

        Session(
                final IOrderBook orderBook,
                final int transactionsNumber,
                final int targetOrderBookOrdersHalf,
                final boolean avalancheIoc,
                final int numUsers,
                final int symbol,
                final boolean enableSlidingPrice,
                final int seed) {
            this.orderBook = orderBook;
            this.targetOrderBookOrdersHalf = targetOrderBookOrdersHalf;
            this.avalancheIoc = avalancheIoc;
            this.numUsers = numUsers;
            this.symbol = symbol;
            // Same seed derivation as the original generator.
            int hash = 1;
            hash = 31 * hash + symbol * -177277;
            hash = 31 * hash + (seed * 10037 + 198267);
            this.rand = new Random(hash);

            final int price = (int) Math.pow(10, 3.3 + rand.nextDouble() * 1.5 + rand.nextDouble() * 1.5);
            this.lastTradePrice = price;
            this.priceDeviation = Math.min((int) (price * 0.05), 10000);
            this.minPrice = price - priceDeviation * 5;
            this.maxPrice = price + priceDeviation * 5;
            this.priceDirection = enableSlidingPrice ? 1 : 0;
            this.lackOrOrdersFastFillThreshold =
                    Math.min(CHECK_ORDERBOOK_STAT_EVERY_NTH_COMMAND, targetOrderBookOrdersHalf * 3 / 4);

            if (transactionsNumber < 0) {
                throw new IllegalArgumentException("transactionsNumber must be >= 0");
            }
        }
    }
}
