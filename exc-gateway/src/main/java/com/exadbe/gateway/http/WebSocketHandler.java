package com.exadbe.gateway.http;

import com.exadbe.gateway.stream.StreamBroadcaster;
import com.exadbe.gateway.stream.StreamSink;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

/**
 * Registers each upgraded WebSocket channel with the {@link StreamBroadcaster}
 * so it receives streamed JSON events. Ping / Pong / Close frames are handled by
 * {@code WebSocketServerProtocolHandler}; this handler only consumes text frames
 * (a client may send an optional subscribe message that the broadcast-all model
 * ignores) and cleans the subscriber up on disconnect.
 */
final class WebSocketHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

    private final StreamBroadcaster broadcaster;
    private final int maxSubscribers;
    private StreamSink sink;

    WebSocketHandler(final StreamBroadcaster broadcaster, final int maxSubscribers) {
        this.broadcaster = broadcaster;
        this.maxSubscribers = maxSubscribers;
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
        if (evt instanceof io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler.HandshakeComplete) {
            if (broadcaster.subscriberCount() >= maxSubscribers) {
                // The subscriber set is unbounded memory and fan-out cost grows
                // with it; refuse handshakes beyond the configured cap.
                ctx.close();
                return;
            }
            final Channel channel = ctx.channel();
            this.sink = json -> {
                // A slow subscriber must not grow the outbound buffer without bound:
                // drop frames once the channel crosses its write watermark. The
                // drop is counted so silent data loss stays observable.
                if (channel.isWritable()) {
                    channel.writeAndFlush(new TextWebSocketFrame(json));
                } else {
                    broadcaster.recordDrop();
                }
            };
            broadcaster.add(sink);
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final WebSocketFrame frame) {
        // Text frames are consumed but not interpreted; every event is broadcast
        // to all subscribers and the client filters by symbol.
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
        remove();
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        remove();
        ctx.close();
    }

    private void remove() {
        if (sink != null) {
            broadcaster.remove(sink);
            sink = null;
        }
    }
}
