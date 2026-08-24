package com.exadbe.gateway.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serves the bundled UI from the classpath {@code static/} directory. It only
 * intercepts requests that are not the JSON API ({@code /api/*}) and not the
 * WebSocket upgrade ({@code /ws}); those are forwarded to the next handler. The
 * route is {@code /} -> {@code static/index.html}, so opening the gateway host
 * lands on the SPA.
 *
 * <p>Boundary code only - no engine, no hot path. Resources are read on demand;
 * a small per-resource cache avoids re-reading the fixed set of UI files.
 */
final class StaticResourceHandler extends ChannelInboundHandlerAdapter {

    private static final Map<String, String> CONTENT_TYPES = new HashMap<>();

    static {
        CONTENT_TYPES.put("html", "text/html; charset=utf-8");
        CONTENT_TYPES.put("js", "application/javascript; charset=utf-8");
        CONTENT_TYPES.put("mjs", "application/javascript; charset=utf-8");
        CONTENT_TYPES.put("css", "text/css; charset=utf-8");
        CONTENT_TYPES.put("svg", "image/svg+xml");
        CONTENT_TYPES.put("png", "image/png");
        CONTENT_TYPES.put("ico", "image/x-icon");
        CONTENT_TYPES.put("json", "application/json; charset=utf-8");
    }

    private final Map<String, byte[]> cache = new ConcurrentHashMap<>();

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        if (!(msg instanceof FullHttpRequest)) {
            ctx.fireChannelRead(msg);
            return;
        }
        final FullHttpRequest request = (FullHttpRequest) msg;
        final String path = new io.netty.handler.codec.http.QueryStringDecoder(request.uri()).path();
        if (path.startsWith("/api") || path.equals("/ws")) {
            // The JSON API and the WebSocket upgrade belong to the router / WS handlers.
            ctx.fireChannelRead(msg);
            return;
        }
        try {
            final byte[] resource = resolve(path);
            if (resource == null) {
                writeJson(ctx, HttpResponseStatus.NOT_FOUND, "not found: " + path);
            } else {
                write(ctx, HttpResponseStatus.OK, contentType(path), resource);
            }
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    /** Maps a request path to a classpath resource under {@code static/}. */
    private byte[] resolve(final String path) {
        final String relative;
        if (path.equals("/") || path.equals("/index.html")) {
            relative = "index.html";
        } else if (path.startsWith("/")) {
            relative = path.substring(1);
        } else {
            relative = path;
        }
        if (relative.isEmpty() || relative.contains("..")) {
            return null;
        }
        final byte[] cached = cache.get(relative);
        if (cached != null) {
            return cached;
        }
        final byte[] loaded = load("static/" + relative);
        if (loaded != null) {
            cache.put(relative, loaded);
        }
        return loaded;
    }

    private static byte[] load(final String resource) {
        try (InputStream in = StaticResourceHandler.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                return null;
            }
            return in.readAllBytes();
        } catch (final java.io.IOException e) {
            return null;
        }
    }

    private static String contentType(final String path) {
        final int dot = path.lastIndexOf('.');
        if (dot < 0) {
            return "application/octet-stream";
        }
        final String ext = path.substring(dot + 1).toLowerCase();
        return CONTENT_TYPES.getOrDefault(ext, "application/octet-stream");
    }

    private static void write(
            final ChannelHandlerContext ctx, final HttpResponseStatus status, final String type, final byte[] bytes) {
        final FullHttpResponse response =
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(bytes));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, type);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private static void writeJson(
            final ChannelHandlerContext ctx, final HttpResponseStatus status, final String message) {
        final byte[] bytes = ("{\"error\":true,\"message\":\"" + message + "\"}").getBytes(StandardCharsets.UTF_8);
        write(ctx, status, "application/json; charset=utf-8", bytes);
    }
}
