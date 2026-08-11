package com.exadbe.gateway.transport;

import com.exadbe.gateway.codec.JsonWriter;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

/**
 * HTTP response helpers shared by the event loops (error fast paths) and the
 * gateway agent (regular responses). Writing through {@code ctx.writeAndFlush}
 * is safe from any thread.
 */
public final class HttpResponses {

    private HttpResponses() {}

    /** Writes a JSON body with the given status; closes the connection when not keep-alive. */
    public static void sendJson(
            final ChannelHandlerContext ctx,
            final HttpResponseStatus status,
            final byte[] data,
            final int length,
            final boolean keepAlive) {
        final ByteBuf content = ctx.alloc().buffer(length);
        content.writeBytes(data, 0, length);
        final FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json;charset=UTF-8");
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, length);
        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            ctx.writeAndFlush(response);
        } else {
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    /** Writes the gateway error envelope for requests rejected before reaching the agent. */
    public static void sendError(
            final ChannelHandlerContext ctx,
            final HttpResponseStatus status,
            final int gatewayResultCode,
            final String description,
            final boolean keepAlive) {
        final JsonWriter json = new JsonWriter(256);
        json.beginObject()
                .name("ticket")
                .valueLong(0L)
                .name("gatewayResultCode")
                .valueLong(gatewayResultCode)
                .name("coreResultCode")
                .valueLong(0L)
                .name("description")
                .valueString(description)
                .name("data")
                .valueNull()
                .endObject();
        sendJson(ctx, status, json.buffer(), json.length(), keepAlive);
    }
}
