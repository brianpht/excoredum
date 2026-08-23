package com.exadbe.gateway.http;

import com.exadbe.gateway.stream.StreamBroadcaster;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import java.net.InetSocketAddress;

/**
 * A minimal Netty HTTP/1.1 + WebSocket JSON server for the gateway. Binds two
 * {@link NioEventLoopGroup}s (accept + IO), decodes HTTP, aggregates the body,
 * and hands each {@link io.netty.handler.codec.http.FullHttpRequest} to the
 * {@link HttpHandler} that dispatches to the {@link Router}. When a
 * {@link StreamBroadcaster} is provided, {@code /ws} negotiates a WebSocket and
 * fans out streamed market events to subscribers.
 */
public final class HttpServer {

    private final String host;
    private final int port;
    private final Router router;
    private final StreamBroadcaster broadcaster;
    private final EventLoopGroup boss = new NioEventLoopGroup(1);
    private final EventLoopGroup worker = new NioEventLoopGroup();
    private Channel serverChannel;

    public HttpServer(final String host, final int port, final Router router) {
        this(host, port, router, null);
    }

    public HttpServer(final String host, final int port, final Router router, final StreamBroadcaster broadcaster) {
        this.host = host;
        this.port = port;
        this.router = router;
        this.broadcaster = broadcaster;
    }

    /** Binds the socket (blocking) and returns once listening. */
    public void start() throws InterruptedException {
        final ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel ch) {
                        ch.pipeline().addLast(new HttpServerCodec());
                        ch.pipeline().addLast(new HttpObjectAggregator(1 << 20));
                        if (broadcaster != null) {
                            ch.pipeline().addLast(new WebSocketServerProtocolHandler("/ws"));
                            ch.pipeline().addLast(new WebSocketHandler(broadcaster));
                        }
                        ch.pipeline().addLast(new HttpHandler(router));
                    }
                });
        this.serverChannel =
                bootstrap.bind(new InetSocketAddress(host, port)).sync().channel();
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly();
        }
        boss.shutdownGracefully();
        worker.shutdownGracefully();
    }

    /** The bound local port (useful when binding to an ephemeral port 0). */
    public int boundPort() {
        return ((java.net.InetSocketAddress) serverChannel.localAddress()).getPort();
    }
}
