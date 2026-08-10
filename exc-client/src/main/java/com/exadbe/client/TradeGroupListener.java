package com.exadbe.client;

/**
 * Optional callback for the fills of one taker command, aggregated into a
 * {@link TradeGroup} and delivered once the command's event stream is
 * complete (its {@code CommandResult.eventCount} frames arrived, or the next
 * command's frames began). This is the client-side view of exchange-core's
 * grouped trade event; the per-fill {@link TradeEventListener} fires
 * independently of it. Invoked on the client's polling thread; implementations
 * must not block. The group holder is reused across callbacks.
 */
@FunctionalInterface
public interface TradeGroupListener {

    TradeGroupListener NONE = group -> {};

    void onTradeGroup(TradeGroup group);
}
