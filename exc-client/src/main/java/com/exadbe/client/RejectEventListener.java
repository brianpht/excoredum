package com.exadbe.client;

/**
 * Optional callback for reject events delivered on the session egress. Emitted
 * when order size could not be matched (IOC / FOK remainder). Invoked on the
 * client's polling thread; implementations must not block.
 */
@FunctionalInterface
public interface RejectEventListener {

    RejectEventListener NONE = (commandIdHi, commandIdLo, eventIndex, symbolId, orderId, uid, rejectedSize) -> {};

    void onReject(
            long commandIdHi,
            long commandIdLo,
            int eventIndex,
            int symbolId,
            long orderId,
            long uid,
            long rejectedSize);
}
