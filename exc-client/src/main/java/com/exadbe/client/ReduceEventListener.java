package com.exadbe.client;

/**
 * Optional callback for reduce events delivered on the session egress. Emitted
 * when a resting order was reduced or cancelled. Invoked on the client's
 * polling thread; implementations must not block.
 */
@FunctionalInterface
public interface ReduceEventListener {

    ReduceEventListener NONE = (commandIdHi, commandIdLo, eventIndex, symbolId, orderId, uid, reducedBy) -> {};

    void onReduce(
            long commandIdHi, long commandIdLo, int eventIndex, int symbolId, long orderId, long uid, long reducedBy);
}
