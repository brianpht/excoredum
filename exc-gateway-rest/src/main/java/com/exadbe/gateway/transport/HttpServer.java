package com.exadbe.gateway.transport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

/**
 * Netty HTTP front end. Event loops only decode HTTP, route, and enqueue pooled
 * request slots; the gateway agent owns every downstream decision. Binding to
 * port {@code 0} picks a free port, readable via {@link #boundPort()}.
 */
public final class HttpServer implements AutoCloseable {

    private final int port;
    private final ManyToOneConcurrentArrayQueue<GatewayRequest> inbound;
    private final ManyToOneConcurrentArrayQueue<GatewayRequest> free;
    private final long requestTimeoutNs;
    private final int maxContentLength;

    private NioEventLoopGroup boss;
    private NioEventLoopGroup workers;
    private Channel channel;

    public HttpServer(
            final int port,
            final ManyToOneConcurrentArrayQueue<GatewayRequest> inbound,
            final ManyToOneConcurrentArrayQueue<GatewayRequest> free,
            final long requestTimeoutNs,
            final int maxContentLength) {
        this.port = port;
        this.inbound = inbound;
        this.free = free;
        this.requestTimeoutNs = requestTimeoutNs;
        this.maxContentLength = maxContentLength;
    }

    public void start() throws InterruptedException {
        boss = new NioEventLoopGroup(1);
        workers = new NioEventLoopGroup();
        final ServerBootstrap bootstrap = new ServerBootstrap()
                .group(boss, workers)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new HttpServerCodec())
                                .addLast(new HttpObjectAggregator(maxContentLength))
                                .addLast(new RouterHandler(inbound, free, requestTimeoutNs));
                    }
                });
        channel = bootstrap.bind(port).sync().channel();
    }

    /** The actual TCP port, useful when constructed with port 0. */
    public int boundPort() {
        return ((InetSocketAddress) channel.localAddress()).getPort();
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.close().awaitUninterruptibly();
        }
        if (boss != null) {
            boss.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS).awaitUninterruptibly();
        }
        if (workers != null) {
            workers.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS).awaitUninterruptibly();
        }
    }
}
