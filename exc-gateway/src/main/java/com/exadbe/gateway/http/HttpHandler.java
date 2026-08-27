package com.exadbe.gateway.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Dispatches each aggregated request to the {@link Router} and writes the JSON
 * response. The produced future completes on the read/write pump thread, so the
 * response is written via {@code ctx.executor().execute(...)} back on this
 * channel's event loop, never blocking it.
 */
final class HttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final Router router;

    HttpHandler(final Router router) {
        this.router = router;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpRequest request) {
        final HandlerRequest handlerRequest = new HandlerRequest(request);
        final boolean keepAlive = HttpUtil.isKeepAlive(request);
        final CompletableFuture<?> future;
        try {
            future = router.route(handlerRequest);
        } catch (final RuntimeException e) {
            respond(ctx, null, e, keepAlive);
            return;
        }
        future.whenComplete((payload, error) -> {
            ctx.executor().execute(() -> respond(ctx, payload, error, keepAlive));
        });
    }

    private void respond(
            final ChannelHandlerContext ctx, final Object payload, final Throwable error, final boolean keepAlive) {
        if (error != null) {
            final Throwable cause = unwrap(error);
            writeJson(ctx, statusOf(cause), errorBody(cause), keepAlive);
        } else {
            writeJson(ctx, 200, payload, keepAlive);
        }
    }

    private void writeJson(
            final ChannelHandlerContext ctx, final int status, final Object payload, final boolean keepAlive) {
        byte[] bytes;
        int effectiveStatus;
        try {
            bytes = Json.write(payload).getBytes(StandardCharsets.UTF_8);
            effectiveStatus = status;
        } catch (final JsonProcessingException e) {
            bytes = "{\"error\":true,\"message\":\"serialization failed\"}".getBytes(StandardCharsets.UTF_8);
            effectiveStatus = 500;
        }
        final FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(effectiveStatus), Unpooled.wrappedBuffer(bytes));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=utf-8");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        if (keepAlive) {
            ctx.writeAndFlush(response);
        } else {
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    private static Object errorBody(final Throwable error) {
        final Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", true);
        body.put("message", error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage());
        return body;
    }

    private static int statusOf(final Throwable error) {
        return error instanceof ApiException ? ((ApiException) error).status() : 500;
    }

    private static Throwable unwrap(final Throwable error) {
        Throwable t = error;
        while ((t instanceof CompletionException || t instanceof ExecutionException) && t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        ctx.close();
    }
}
