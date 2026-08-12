package com.exadbe.gateway.transport;

import com.exadbe.gateway.codec.JsonReader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

/**
 * Netty inbound handler for WebSocket text frames on the real-time market data
 * channel. Event loops only decode the flat JSON operation object and enqueue
 * a pooled {@link WsEvent} slot for the gateway agent; every decision (symbol
 * resolution, bounds, ack/error replies) happens on the agent thread. The
 * connection is dropped when the inbound queue is full, applying backpressure
 * to a misbehaving client.
 */
public final class WebSocketHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {

    private static final String OP_SUBSCRIBE = "subscribe";
    private static final String OP_UNSUBSCRIBE = "unsubscribe";
    private static final String OP_ORDER_BOOK = "orderBook";
    private static final String CHANNEL_TICKS = "ticks";
    private static final String CHANNEL_ORDERS = "orders";

    private final ManyToOneConcurrentArrayQueue<WsEvent> inbound;
    private final ManyToOneConcurrentArrayQueue<WsEvent> free;
    private final JsonReader reader = new JsonReader();

    public WebSocketHandler(
            final ManyToOneConcurrentArrayQueue<WsEvent> inbound, final ManyToOneConcurrentArrayQueue<WsEvent> free) {
        this.inbound = inbound;
        this.free = free;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final TextWebSocketFrame frame) {
        final WsEvent slot = free.poll();
        if (slot == null) {
            ctx.close();
            return;
        }
        slot.ctx = ctx;
        final ByteBuf content = frame.content();
        final int length = content.readableBytes();
        if (length > 0) {
            parse(slot, content, length);
        } else {
            slot.kind = WsEvent.INVALID_OP;
        }
        if (!inbound.offer(slot)) {
            slot.reset();
            free.offer(slot);
            ctx.close();
        }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
        final WsEvent slot = free.poll();
        if (slot == null) {
            return;
        }
        slot.kind = WsEvent.DISCONNECT;
        slot.ctx = ctx;
        if (!inbound.offer(slot)) {
            slot.reset();
            free.offer(slot);
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        ctx.close();
    }

    private void parse(final WsEvent slot, final ByteBuf content, final int length) {
        final byte[] bytes = new byte[length];
        content.getBytes(content.readerIndex(), bytes);
        reader.wrap(bytes, length);
        if (!reader.beginObject()) {
            slot.kind = WsEvent.INVALID_OP;
            return;
        }
        String op = null;
        String channel = null;
        while (reader.hasNextField()) {
            final String name = reader.fieldName();
            switch (name) {
                case "op":
                    op = reader.nextString();
                    break;
                case "channel":
                    channel = reader.nextString();
                    break;
                case "symbol":
                    slot.symbolCode = reader.nextString();
                    break;
                case "uid":
                    slot.uid = reader.nextLong();
                    break;
                case "depth":
                    slot.depth = (int) reader.nextLong();
                    break;
                default:
                    reader.skipValue();
                    break;
            }
            if (reader.failed()) {
                slot.kind = WsEvent.INVALID_OP;
                return;
            }
        }
        if (reader.failed() || op == null || op.isEmpty()) {
            slot.kind = WsEvent.INVALID_OP;
            return;
        }
        if (OP_SUBSCRIBE.equals(op)) {
            slot.kind = CHANNEL_TICKS.equals(channel)
                    ? WsEvent.SUBSCRIBE_TICKS
                    : CHANNEL_ORDERS.equals(channel) ? WsEvent.SUBSCRIBE_ORDERS : WsEvent.INVALID_OP;
            return;
        }
        if (OP_UNSUBSCRIBE.equals(op)) {
            slot.kind = CHANNEL_TICKS.equals(channel)
                    ? WsEvent.UNSUBSCRIBE_TICKS
                    : CHANNEL_ORDERS.equals(channel) ? WsEvent.UNSUBSCRIBE_ORDERS : WsEvent.INVALID_OP;
            return;
        }
        slot.kind = OP_ORDER_BOOK.equals(op) ? WsEvent.ORDER_BOOK_REQUEST : WsEvent.INVALID_OP;
    }
}
