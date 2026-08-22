package com.exadbe.xcorebench;

import com.exadbe.core.CommandOutcome;
import com.exadbe.engine.orderbook.L2View;
import com.exadbe.engine.orderbook.OrderBookNaive;
import com.exadbe.engine.risk.SymbolSpec;
import com.exadbe.protocol.CommandResultCode;

/** Replays a {@link Workload} through excoredum's {@link OrderBookNaive}. */
public final class ExcBookRunner {

    private ExcBookRunner() {}

    /** Replays the full workload against a fresh book and returns the digest. */
    public static BookStats replay(final Workload workload, final int symbolId) {
        final OrderBookNaive book = new OrderBookNaive(symbolId);
        final CommandOutcome outcome = new CommandOutcome(1024);
        // MOVE validation only consults the spec's fee floor and overflow bounds;
        // unit scales and zero fees keep the replay's behavior unchanged.
        final SymbolSpec spec = new SymbolSpec(symbolId, 0, 1, 1L, 1L, 0L, 0L);

        long trades = 0;
        long tradeVolume = 0;
        long rejects = 0;
        long rejectedSize = 0;
        long reduces = 0;
        long reducedSize = 0;

        final int count = workload.count();
        final long began = System.nanoTime();
        for (int i = 0; i < count; i++) {
            outcome.reset(0L, i);
            final byte type = workload.type(i);
            if (type == Workload.PLACE) {
                final byte orderType = workload.orderType(i);
                if (orderType == Workload.GTC) {
                    book.placeGtc(
                            workload.orderId(i),
                            workload.ask(i),
                            workload.price(i),
                            workload.size(i),
                            workload.reservePrice(i),
                            workload.uid(i),
                            i,
                            outcome);
                } else if (orderType == Workload.IOC) {
                    book.matchIoc(
                            workload.orderId(i),
                            workload.ask(i),
                            workload.price(i),
                            workload.size(i),
                            workload.reservePrice(i),
                            workload.uid(i),
                            outcome);
                } else {
                    book.matchFokBudget(
                            workload.orderId(i),
                            workload.ask(i),
                            workload.price(i),
                            workload.size(i),
                            workload.reservePrice(i),
                            workload.uid(i),
                            outcome);
                }
            } else if (type == Workload.CANCEL) {
                require(book.cancel(workload.orderId(i), workload.uid(i), outcome), "cancel", i);
            } else if (type == Workload.MOVE) {
                require(book.move(workload.orderId(i), workload.uid(i), workload.price(i), spec, outcome), "move", i);
            } else {
                require(book.reduce(workload.orderId(i), workload.uid(i), workload.size(i), outcome), "reduce", i);
            }

            for (int e = 0; e < outcome.eventCount(); e++) {
                final CommandOutcome.EventRecord event = outcome.event(e);
                switch (event.kind()) {
                    case TRADE -> {
                        trades++;
                        tradeVolume += event.size();
                    }
                    case REDUCE -> {
                        reduces++;
                        reducedSize += event.size();
                    }
                    case REJECT -> {
                        rejects++;
                        rejectedSize += event.size();
                    }
                }
            }
        }
        final long replayNanos = System.nanoTime() - began;

        final L2View l2 = new L2View(count + 16);
        book.fillL2(l2);
        long checksum = 0xCBF29CE484222325L;
        for (int i = 0; i < l2.askDepth(); i++) {
            checksum = BookStats.mixLevel(checksum, l2.askPrice(i), l2.askVolume(i));
        }
        for (int i = 0; i < l2.bidDepth(); i++) {
            checksum = BookStats.mixLevel(checksum, l2.bidPrice(i), l2.bidVolume(i));
        }

        long askVolumeTotal = 0;
        for (int i = 0; i < l2.askDepth(); i++) {
            askVolumeTotal += l2.askVolume(i);
        }
        long bidVolumeTotal = 0;
        for (int i = 0; i < l2.bidDepth(); i++) {
            bidVolumeTotal += l2.bidVolume(i);
        }

        return new BookStats(
                "exc-core OrderBookNaive",
                replayNanos,
                trades,
                tradeVolume,
                rejects,
                rejectedSize,
                reduces,
                reducedSize,
                l2.askDepth(),
                l2.bidDepth(),
                askVolumeTotal,
                bidVolumeTotal,
                checksum);
    }

    private static void require(final CommandResultCode code, final String what, final int index) {
        if (code != CommandResultCode.SUCCESS) {
            throw new IllegalStateException(what + " at command " + index + " failed: " + code);
        }
    }
}
