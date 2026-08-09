package com.exadbe.engine.orderbook;

import org.agrona.collections.Long2ObjectHashMap;

/**
 * One side of the book: price buckets kept in a doubly-linked list ordered
 * best-first, with a {@link Long2ObjectHashMap} for O(1) price lookup. For the
 * ask side the best price is the lowest; for the bid side, the highest.
 */
final class OrderBookSide {

    private final boolean ask;
    private final PriceBucketPool bucketPool;
    private final Long2ObjectHashMap<PriceBucket> byPrice = new Long2ObjectHashMap<>();

    private PriceBucket best;

    OrderBookSide(final boolean ask, final PriceBucketPool bucketPool) {
        this.ask = ask;
        this.bucketPool = bucketPool;
    }

    PriceBucket best() {
        return best;
    }

    PriceBucket find(final long price) {
        return byPrice.get(price);
    }

    int bucketCount() {
        return byPrice.size();
    }

    /** Returns the bucket for {@code price}, inserting a pooled one in sorted order. */
    PriceBucket getOrCreate(final long price) {
        final PriceBucket existing = byPrice.get(price);
        if (existing != null) {
            return existing;
        }
        final PriceBucket bucket = bucketPool.acquire();
        bucket.reset(price);
        insertSorted(bucket);
        byPrice.put(price, bucket);
        return bucket;
    }

    void remove(final PriceBucket bucket) {
        final PriceBucket better = bucket.better;
        final PriceBucket worse = bucket.worse;
        if (better == null) {
            best = worse;
        } else {
            better.worse = worse;
        }
        if (worse != null) {
            worse.better = better;
        }
        byPrice.remove(bucket.price);
        bucketPool.release(bucket);
    }

    private void insertSorted(final PriceBucket bucket) {
        if (best == null) {
            best = bucket;
            return;
        }
        PriceBucket prev = null;
        PriceBucket cursor = best;
        while (cursor != null && isBetter(cursor.price, bucket.price)) {
            prev = cursor;
            cursor = cursor.worse;
        }
        bucket.better = prev;
        bucket.worse = cursor;
        if (prev == null) {
            best = bucket;
        } else {
            prev.worse = bucket;
        }
        if (cursor != null) {
            cursor.better = bucket;
        }
    }

    private boolean isBetter(final long a, final long b) {
        return ask ? a < b : a > b;
    }

    /** True if a bucket at {@code bucketPrice} is marketable against {@code limitPrice} for a taker. */
    boolean marketable(final long bucketPrice, final long limitPrice) {
        // This side holds the makers; ask makers fill a buying taker priced >= ask,
        // bid makers fill a selling taker priced <= bid.
        return ask ? bucketPrice <= limitPrice : bucketPrice >= limitPrice;
    }
}
