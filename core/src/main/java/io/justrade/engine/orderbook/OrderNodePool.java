package io.justrade.engine.orderbook;

/**
 * Single-writer object pool for {@link OrderNode}s, reused across all books owned
 * by one engine. A free stack retains released nodes up to {@code capacity}; on an
 * empty stack {@link #acquire()} allocates (cold path) and bumps an exhaustion
 * counter so an operator can detect an undersized pool. After warmup the resting
 * set is bounded, so steady-state matching allocates zero nodes.
 */
public final class OrderNodePool {

    private final OrderNode[] free;
    private int top;
    private long allocations;

    public OrderNodePool(final int capacity) {
        this.free = new OrderNode[Math.max(1, capacity)];
    }

    /** Returns a node to fill via {@link OrderNode#set}; never null. */
    OrderNode acquire() {
        if (top == 0) {
            allocations++;
            return new OrderNode();
        }
        final OrderNode node = free[--top];
        free[top] = null;
        return node;
    }

    /** Returns a node to the pool; dropped for GC if the pool is already full. */
    void release(final OrderNode node) {
        if (top < free.length) {
            free[top++] = node;
        }
    }

    /** Cumulative cold-path allocations made because the pool was empty. */
    public long allocations() {
        return allocations;
    }
}
