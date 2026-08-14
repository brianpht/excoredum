package com.exadbe.write.client;

/**
 * Optional callback for trade events delivered on the session egress. Invoked on
 * the client's polling thread; implementations must not block.
 */
@FunctionalInterface
public interface TradeEventListener {

    TradeEventListener NONE = (commandIdHi,
            commandIdLo,
            eventIndex,
            symbolId,
            makerOrderId,
            makerUid,
            takerUid,
            price,
            size,
            makerCompleted) -> {};

    void onTrade(
            long commandIdHi,
            long commandIdLo,
            int eventIndex,
            int symbolId,
            long makerOrderId,
            long makerUid,
            long takerUid,
            long price,
            long size,
            boolean makerCompleted);
}
