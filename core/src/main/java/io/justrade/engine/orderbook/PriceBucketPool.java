package io.justrade.engine.orderbook;

/**
 * Single-writer object pool for {@link PriceBucket}s, reused across all books
 * owned by one engine. A free stack retains released buckets up to
 * {@code capacity}; on an empty stack {@link #acquire()} allocates (cold path)
 * and bumps an exhaustion counter so an operator can detect an undersized pool.
 *
 * <p>Price levels come and go as the book's depth shifts, so without pooling
 * every fresh price level allocates a bucket and every emptied level discards
 * one. After warmup the live level count is bounded, so steady-state matching
 * allocates zero buckets. Mirrors {@link OrderNodePool}.
 */
public final class PriceBucketPool {

    private final PriceBucket[] free;
    private int top;
    private long allocations;

    public PriceBucketPool(final int capacity) {
        this.free = new PriceBucket[Math.max(1, capacity)];
    }

    /** Returns a bucket to fill via {@link PriceBucket#reset}; never null. */
    PriceBucket acquire() {
        if (top == 0) {
            allocations++;
            return new PriceBucket();
        }
        final PriceBucket bucket = free[--top];
        free[top] = null;
        return bucket;
    }

    /** Returns a bucket to the pool; dropped for GC if the pool is already full. */
    void release(final PriceBucket bucket) {
        if (top < free.length) {
            free[top++] = bucket;
        }
    }

    /** Cumulative cold-path allocations made because the pool was empty. */
    public long allocations() {
        return allocations;
    }
}
