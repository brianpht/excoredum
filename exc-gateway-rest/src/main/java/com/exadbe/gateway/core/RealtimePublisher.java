package com.exadbe.gateway.core;

import com.exadbe.engine.orderbook.L2View;
import com.exadbe.gateway.codec.JsonWriter;
import com.exadbe.gateway.transport.WsEvent;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * WebSocket fan-out owned by the gateway agent thread. Holds the subscriber
 * index ({@link WsSubscriptions}) plus the channel per subscriber id, and
 * serializes every outbound frame with the shared {@link JsonWriter}: one
 * frame per write, copied out of the reusable buffer so a later frame can
 * never corrupt an in-flight write. Tick frames are dropped for consumers
 * whose channel is not writable (slow consumer), keeping the agent's fan-out
 * bounded.
 */
public final class RealtimePublisher {

    private final GatewayState state;
    private final WsSubscriptions subscriptions;
    private final Object2ObjectHashMap<ChannelId, Channel> channelsById = new Object2ObjectHashMap<>();
    private final JsonWriter json = new JsonWriter(1024);

    public RealtimePublisher(final GatewayState state, final WsSubscriptions subscriptions) {
        this.state = state;
        this.subscriptions = subscriptions;
    }

    public int subscribeTicks(final ChannelHandlerContext ctx, final int symbolId) {
        final ChannelId channelId = ctx.channel().id();
        final int result = subscriptions.subscribeTicks(channelId, symbolId);
        if (result == WsSubscriptions.OK) {
            channelsById.put(channelId, ctx.channel());
        }
        return result;
    }

    public int subscribeOrders(final ChannelHandlerContext ctx, final long uid) {
        final ChannelId channelId = ctx.channel().id();
        final int result = subscriptions.subscribeOrders(channelId, uid);
        if (result == WsSubscriptions.OK) {
            channelsById.put(channelId, ctx.channel());
        }
        return result;
    }

    public void unsubscribeTicks(final ChannelHandlerContext ctx, final int symbolId) {
        subscriptions.unsubscribeTicks(ctx.channel().id(), symbolId);
    }

    public void unsubscribeOrders(final ChannelHandlerContext ctx, final long uid) {
        subscriptions.unsubscribeOrders(ctx.channel().id(), uid);
    }

    /** Drops every subscription of the connection; called on disconnect. */
    public void disconnect(final ChannelHandlerContext ctx) {
        final ChannelId channelId = ctx.channel().id();
        subscriptions.disconnect(channelId);
        channelsById.remove(channelId);
    }

    /** Pushes one trade tick to every subscriber of the symbol, if any. */
    public void pushTick(final int symbolId, final long price, final long size, final long timestamp) {
        if (!subscriptions.hasTicksSubscribers(symbolId)) {
            return;
        }
        final GatewaySymbolSpec spec = state.getSymbolSpec(symbolId);
        json.reset();
        writeTickFrame(json, spec, price, size, timestamp);
        subscriptions.forEachTickSubscriber(symbolId, this::writeFrame);
    }

    /** Pushes one order state change to every subscriber of the user, if any. */
    public void pushOrderUpdate(final long uid, final GatewayOrder order) {
        if (!subscriptions.hasOrderSubscribers(uid)) {
            return;
        }
        final GatewaySymbolSpec spec = state.getSymbolSpec(order.symbolId());
        final int priceScale = spec == null ? 0 : spec.quoteCurrency().scale();
        json.reset();
        writeOrderUpdateFrame(json, uid, order, priceScale);
        subscriptions.forEachOrderSubscriber(uid, this::writeFrame);
    }

    /** Answers an L2 order-book snapshot request on the requester's connection. */
    public void pushOrderBook(
            final ChannelHandlerContext ctx, final GatewaySymbolSpec spec, final L2View view, final int depth) {
        json.reset();
        json.beginObject().name("type").valueString("orderBook");
        writeOrderBookData(json, spec, view, depth);
        json.endObject();
        writeTo(ctx.channel());
    }

    public void ack(
            final ChannelHandlerContext ctx,
            final String op,
            final String channel,
            final String symbolCode,
            final long uid) {
        json.reset();
        json.beginObject()
                .name("type")
                .valueString("ack")
                .name("op")
                .valueString(op)
                .name("channel")
                .valueString(channel);
        if (symbolCode != null) {
            json.name("symbol").valueString(symbolCode);
        }
        if (uid != WsEvent.ABSENT) {
            json.name("uid").valueLong(uid);
        }
        json.endObject();
        writeTo(ctx.channel());
    }

    public void error(final ChannelHandlerContext ctx, final int code, final String description) {
        json.reset();
        json.beginObject()
                .name("type")
                .valueString("error")
                .name("code")
                .valueLong(code)
                .name("description")
                .valueString(description)
                .endObject();
        writeTo(ctx.channel());
    }

    /** Writes the tick frame body for a scaled price; spec may be null when the symbol is not registered. */
    public static void writeTickFrame(
            final JsonWriter json,
            final GatewaySymbolSpec spec,
            final long price,
            final long size,
            final long timestamp) {
        final int quoteScale = spec == null ? 0 : spec.quoteCurrency().scale();
        json.beginObject()
                .name("type")
                .valueString("tick")
                .name("symbol")
                .valueString(spec == null ? null : spec.symbolCode())
                .name("price")
                .valueDecimal(price, quoteScale)
                .name("volume")
                .valueLong(size)
                .name("timestamp")
                .valueLong(timestamp)
                .endObject();
    }

    /** Writes the order update frame body; {@code priceScale} comes from the symbol spec. */
    public static void writeOrderUpdateFrame(
            final JsonWriter json, final long uid, final GatewayOrder order, final int priceScale) {
        json.beginObject()
                .name("type")
                .valueString("orderUpdate")
                .name("uid")
                .valueLong(uid)
                .name("orderId")
                .valueLong(order.orderId())
                .name("symbol")
                .valueString(order.symbolCode())
                .name("price")
                .valueDecimal(order.price(), priceScale)
                .name("size")
                .valueLong(order.size())
                .name("filled")
                .valueLong(order.filled())
                .name("state")
                .valueString(order.stateName())
                .name("userCookie")
                .valueLong(order.userCookie())
                .name("action")
                .valueString(order.ask() ? "ASK" : "BID")
                .name("orderType")
                .valueString(order.orderType())
                .endObject();
    }

    /** Writes the L2 book arrays shared by the REST order book endpoint and the WebSocket snapshot. */
    public static void writeOrderBookData(
            final JsonWriter json, final GatewaySymbolSpec spec, final L2View view, final int depth) {
        final int quoteScale = spec.quoteCurrency().scale();
        json.name("symbol").valueString(spec.symbolCode()).name("askPrices").beginArray();
        final int asks = Math.min(view.askDepth(), depth);
        for (int i = 0; i < asks; i++) {
            json.valueDecimal(view.askPrice(i), quoteScale);
        }
        json.endArray().name("askVolumes").beginArray();
        for (int i = 0; i < asks; i++) {
            json.valueLong(view.askVolume(i));
        }
        json.endArray().name("bidPrices").beginArray();
        final int bids = Math.min(view.bidDepth(), depth);
        for (int i = 0; i < bids; i++) {
            json.valueDecimal(view.bidPrice(i), quoteScale);
        }
        json.endArray().name("bidVolumes").beginArray();
        for (int i = 0; i < bids; i++) {
            json.valueLong(view.bidVolume(i));
        }
        json.endArray();
    }

    private void writeFrame(final ChannelId channelId) {
        final Channel channel = channelsById.get(channelId);
        if (channel == null || !channel.isWritable()) {
            return;
        }
        writeTo(channel);
    }

    private void writeTo(final Channel channel) {
        // The shared writer buffer is reused for the next frame; copy per frame
        // so an async Netty write never aliases it.
        channel.writeAndFlush(new TextWebSocketFrame(Unpooled.copiedBuffer(json.buffer(), 0, json.length())));
    }
}
