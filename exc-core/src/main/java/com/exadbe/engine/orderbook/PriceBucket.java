package com.exadbe.engine.orderbook;

/**
 * A single price level: a FIFO queue of resting orders at one price, plus links
 * to the adjacent price levels on the same side (sorted best-first).
 */
final class PriceBucket {

    long price;
    long totalVolume;
    int numOrders;

    OrderNode firstOrder;
    OrderNode lastOrder;

    // Neighbours in the side's price ordering: better is toward the best price.
    PriceBucket better;
    PriceBucket worse;

    void reset(final long price) {
        this.price = price;
        this.totalVolume = 0L;
        this.numOrders = 0;
        this.firstOrder = null;
        this.lastOrder = null;
        this.better = null;
        this.worse = null;
    }

    void append(final OrderNode order) {
        order.bucketPrev = lastOrder;
        order.bucketNext = null;
        if (lastOrder == null) {
            firstOrder = order;
        } else {
            lastOrder.bucketNext = order;
        }
        lastOrder = order;
        totalVolume += order.remaining();
        numOrders++;
    }

    void removeOrder(final OrderNode order) {
        final OrderNode prev = order.bucketPrev;
        final OrderNode next = order.bucketNext;
        if (prev == null) {
            firstOrder = next;
        } else {
            prev.bucketNext = next;
        }
        if (next == null) {
            lastOrder = prev;
        } else {
            next.bucketPrev = prev;
        }
        totalVolume -= order.remaining();
        numOrders--;
    }
}
