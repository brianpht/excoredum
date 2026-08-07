package com.exadbe.engine.orderbook;

/**
 * A resting limit order. Carries intrusive FIFO links so its bucket can enqueue
 * and remove it in O(1) without a per-order map.
 */
final class OrderNode {

    long orderId;
    long price;
    long size;
    long filled;
    long reserveBidPrice;
    long uid;
    long timestamp;
    boolean ask;

    OrderNode bucketNext;
    OrderNode bucketPrev;

    long remaining() {
        return size - filled;
    }

    void set(
            final long orderId,
            final long price,
            final long size,
            final long filled,
            final long reserveBidPrice,
            final long uid,
            final long timestamp,
            final boolean ask) {
        this.orderId = orderId;
        this.price = price;
        this.size = size;
        this.filled = filled;
        this.reserveBidPrice = reserveBidPrice;
        this.uid = uid;
        this.timestamp = timestamp;
        this.ask = ask;
        this.bucketNext = null;
        this.bucketPrev = null;
    }
}
