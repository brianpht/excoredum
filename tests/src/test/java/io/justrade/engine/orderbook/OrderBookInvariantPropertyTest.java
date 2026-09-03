package io.justrade.engine.orderbook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.core.CommandOutcome;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/**
 * Property test: replaying a random sequence of non-crossing place / cancel /
 * reduce operations into {@link OrderBookNaive} keeps every structural
 * invariant. Bids and asks occupy disjoint price bands so no trade ever occurs,
 * which lets a plain reference model track the exact resting state that the book
 * must agree with after each step.
 */
class OrderBookInvariantPropertyTest {

    private static final int SYMBOL = 1;
    private static final long UID = 7L;
    private static final int MAX_LEVELS = 128;
    private static final long BID_BASE = 100L;
    private static final long ASK_BASE = 200L;

    private enum Kind {
        PLACE,
        CANCEL,
        REDUCE
    }

    private record Op(Kind kind, long id, boolean ask, long price, long size) {}

    private static final class Resting {
        final boolean ask;
        long remaining;

        Resting(final boolean ask, final long remaining) {
            this.ask = ask;
            this.remaining = remaining;
        }
    }

    @Property(tries = 400)
    void bookInvariantsHold(@ForAll("operations") final List<Op> operations) {
        final OrderBookNaive book = new OrderBookNaive(SYMBOL);
        final Map<Long, Resting> model = new HashMap<>();
        for (final Op op : operations) {
            apply(book, model, op);
            checkInvariants(book, model);
        }
    }

    private static void apply(final OrderBookNaive book, final Map<Long, Resting> model, final Op op) {
        final CommandOutcome out = new CommandOutcome();
        switch (op.kind()) {
            case PLACE -> {
                book.placeGtc(op.id(), op.ask(), op.price(), op.size(), op.price(), UID, 0L, out);
                // A duplicate id is rejected by the book (no rest), so the model
                // only records the first placement of an id.
                model.putIfAbsent(op.id(), new Resting(op.ask(), op.size()));
            }
            case CANCEL -> {
                book.cancel(op.id(), UID, out);
                model.remove(op.id());
            }
            case REDUCE -> {
                book.reduce(op.id(), UID, op.size(), out);
                final Resting r = model.get(op.id());
                if (r != null) {
                    final long reduceBy = Math.min(r.remaining, op.size());
                    if (reduceBy == r.remaining) {
                        model.remove(op.id());
                    } else {
                        r.remaining -= reduceBy;
                    }
                }
            }
            default -> throw new IllegalStateException();
        }
    }

    private static void checkInvariants(final OrderBookNaive book, final Map<Long, Resting> model) {
        assertTrue(book.bestBid() < book.bestAsk(), "book must never be crossed");
        assertEquals(model.size(), book.orderCount(), "resting order count");
        for (final Long id : model.keySet()) {
            assertTrue(book.contains(id), "book must contain live order " + id);
        }

        final L2View view = new L2View(MAX_LEVELS);
        book.fillL2(view);
        assertEquals(book.askBucketCount(), view.askDepth(), "ask levels");
        assertEquals(book.bidBucketCount(), view.bidDepth(), "bid levels");
        assertTrue(view.askDepth() <= MAX_LEVELS && view.bidDepth() <= MAX_LEVELS);

        long askVolume = 0L;
        int askOrders = 0;
        long prevAsk = Long.MIN_VALUE;
        for (int i = 0; i < view.askDepth(); i++) {
            assertTrue(view.askPrice(i) > prevAsk, "ask prices strictly ascending");
            prevAsk = view.askPrice(i);
            assertTrue(view.askVolume(i) > 0L && view.askOrders(i) > 0, "non-empty ask level");
            askVolume += view.askVolume(i);
            askOrders += view.askOrders(i);
        }

        long bidVolume = 0L;
        int bidOrders = 0;
        long prevBid = Long.MAX_VALUE;
        for (int i = 0; i < view.bidDepth(); i++) {
            assertTrue(view.bidPrice(i) < prevBid, "bid prices strictly descending");
            prevBid = view.bidPrice(i);
            assertTrue(view.bidVolume(i) > 0L && view.bidOrders(i) > 0, "non-empty bid level");
            bidVolume += view.bidVolume(i);
            bidOrders += view.bidOrders(i);
        }

        long expectedAskVolume = 0L;
        long expectedBidVolume = 0L;
        int expectedAskOrders = 0;
        int expectedBidOrders = 0;
        for (final Resting r : model.values()) {
            if (r.ask) {
                expectedAskVolume += r.remaining;
                expectedAskOrders++;
            } else {
                expectedBidVolume += r.remaining;
                expectedBidOrders++;
            }
        }
        assertEquals(expectedAskOrders, askOrders, "total ask orders");
        assertEquals(expectedBidOrders, bidOrders, "total bid orders");
        assertEquals(expectedAskVolume, askVolume, "total ask volume");
        assertEquals(expectedBidVolume, bidVolume, "total bid volume");

        assertEquals(view.askDepth() > 0 ? view.askPrice(0) : Long.MAX_VALUE, book.bestAsk(), "best ask");
        assertEquals(view.bidDepth() > 0 ? view.bidPrice(0) : Long.MIN_VALUE, book.bestBid(), "best bid");
    }

    @Provide
    Arbitrary<List<Op>> operations() {
        final Arbitrary<Long> ids = Arbitraries.longs().between(0L, 15L);
        final Arbitrary<Op> place = Combinators.combine(
                        ids,
                        Arbitraries.of(Boolean.TRUE, Boolean.FALSE),
                        Arbitraries.longs().between(0L, 99L),
                        Arbitraries.longs().between(1L, 100L))
                .as((id, ask, offset, size) -> new Op(Kind.PLACE, id, ask, (ask ? ASK_BASE : BID_BASE) + offset, size));
        final Arbitrary<Op> cancel = ids.map(id -> new Op(Kind.CANCEL, id, false, 0L, 0L));
        final Arbitrary<Op> reduce = Combinators.combine(
                        ids, Arbitraries.longs().between(1L, 120L))
                .as((id, size) -> new Op(Kind.REDUCE, id, false, 0L, size));
        return Arbitraries.oneOf(place, cancel, reduce).list().ofMaxSize(60);
    }
}
