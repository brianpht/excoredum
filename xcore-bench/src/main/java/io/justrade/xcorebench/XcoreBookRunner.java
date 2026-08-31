package io.justrade.xcorebench;

import exchange.core2.core.common.L2MarketData;
import exchange.core2.core.common.MatcherEventType;
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.orderbook.IOrderBook;

/**
 * Replays a {@link Workload} through an exchange-core {@link IOrderBook},
 * reusing one mutable {@link OrderCommand} exactly as exchange-core's own
 * ITOrderBookBase benchmark does.
 */
public final class XcoreBookRunner {

    private XcoreBookRunner() {}

    /** Replays the full workload against a fresh book and returns the digest. */
    public static BookStats replay(final Workload workload, final int symbolId, final BookFactory factory) {
        final IOrderBook book = factory.create(symbolId);
        final OrderCommand cmd = OrderCommand.builder().build();

        long trades = 0;
        long tradeVolume = 0;
        long rejects = 0;
        long rejectedSize = 0;
        long reduces = 0;
        long reducedSize = 0;

        final int count = workload.count();
        final long began = System.nanoTime();
        for (int i = 0; i < count; i++) {
            final byte type = workload.type(i);
            cmd.matcherEvent = null;
            cmd.symbol = symbolId;
            cmd.uid = workload.uid(i);
            cmd.orderId = workload.orderId(i);
            cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;

            if (type == Workload.PLACE) {
                cmd.command = OrderCommandType.PLACE_ORDER;
                cmd.action = workload.ask(i) ? OrderAction.ASK : OrderAction.BID;
                cmd.orderType = toXcoreOrderType(workload.orderType(i));
                cmd.price = workload.price(i);
                cmd.size = workload.size(i);
                cmd.reserveBidPrice = workload.reservePrice(i);
            } else if (type == Workload.CANCEL) {
                cmd.command = OrderCommandType.CANCEL_ORDER;
            } else if (type == Workload.MOVE) {
                cmd.command = OrderCommandType.MOVE_ORDER;
                cmd.price = workload.price(i);
            } else {
                cmd.command = OrderCommandType.REDUCE_ORDER;
                cmd.size = workload.size(i);
            }

            final CommandResultCode code = IOrderBook.processCommand(book, cmd);
            if (code != CommandResultCode.SUCCESS) {
                throw new IllegalStateException(
                        factory.name() + " command " + i + " (" + cmd.command + ") failed: " + code);
            }

            MatcherTradeEvent event = cmd.matcherEvent;
            while (event != null) {
                if (event.eventType == MatcherEventType.TRADE) {
                    trades++;
                    tradeVolume += event.size;
                } else if (event.eventType == MatcherEventType.REJECT) {
                    rejects++;
                    rejectedSize += event.size;
                } else if (event.eventType == MatcherEventType.REDUCE) {
                    reduces++;
                    reducedSize += event.size;
                }
                event = event.nextEvent;
            }
        }
        final long replayNanos = System.nanoTime() - began;

        final L2MarketData l2 = book.getL2MarketDataSnapshot(Integer.MAX_VALUE);
        long checksum = 0xCBF29CE484222325L;
        long askVolumeTotal = 0;
        for (int i = 0; i < l2.askSize; i++) {
            checksum = BookStats.mixLevel(checksum, l2.askPrices[i], l2.askVolumes[i]);
            askVolumeTotal += l2.askVolumes[i];
        }
        long bidVolumeTotal = 0;
        for (int i = 0; i < l2.bidSize; i++) {
            checksum = BookStats.mixLevel(checksum, l2.bidPrices[i], l2.bidVolumes[i]);
            bidVolumeTotal += l2.bidVolumes[i];
        }

        return new BookStats(
                factory.name(),
                replayNanos,
                trades,
                tradeVolume,
                rejects,
                rejectedSize,
                reduces,
                reducedSize,
                l2.askSize,
                l2.bidSize,
                askVolumeTotal,
                bidVolumeTotal,
                checksum);
    }

    private static OrderType toXcoreOrderType(final byte orderType) {
        if (orderType == Workload.GTC) {
            return OrderType.GTC;
        }
        if (orderType == Workload.IOC) {
            return OrderType.IOC;
        }
        return OrderType.FOK_BUDGET;
    }

    /** Factory for a fresh exchange-core order book of one implementation. */
    public interface BookFactory {
        IOrderBook create(int symbolId);

        String name();
    }
}
