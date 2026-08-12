package com.exadbe.gateway.transport;

import io.netty.channel.ChannelHandlerContext;

/**
 * A pooled WebSocket event descriptor handed from a Netty event loop to the
 * gateway agent over a lock-free queue. Exactly one thread owns a slot at a
 * time: an event loop fills it, the agent drains and answers it, then returns
 * it to the free queue. All mutable fields are reset on release.
 */
public final class WsEvent {

    /** Event: subscribe to trade ticks for a symbol. */
    public static final int SUBSCRIBE_TICKS = 1;

    /** Event: unsubscribe from trade ticks for a symbol. */
    public static final int UNSUBSCRIBE_TICKS = 2;

    /** Event: subscribe to order updates for a user. */
    public static final int SUBSCRIBE_ORDERS = 3;

    /** Event: unsubscribe from order updates for a user. */
    public static final int UNSUBSCRIBE_ORDERS = 4;

    /** Event: request an L2 order-book snapshot for a symbol. */
    public static final int ORDER_BOOK_REQUEST = 5;

    /** Event: the connection closed; drop all of its subscriptions. */
    public static final int DISCONNECT = 6;

    /** Sentinel for an unparseable or unknown operation frame. */
    public static final int INVALID_OP = 0;

    /** Sentinel marking an absent long field. */
    public static final long ABSENT = Long.MIN_VALUE;

    public int kind;
    public ChannelHandlerContext ctx;
    public String symbolCode;
    public long uid = ABSENT;
    public int depth;

    /** Clears all fields for reuse. */
    public void reset() {
        kind = 0;
        ctx = null;
        symbolCode = null;
        uid = ABSENT;
        depth = 0;
    }
}
