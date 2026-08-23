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
    private StreamSink sink;

    WebSocketHandler(final StreamBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
        if (evt instanceof io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler.HandshakeComplete) {
            final Channel channel = ctx.channel();
            this.sink = json -> channel.writeAndFlush(new TextWebSocketFrame(json));
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
