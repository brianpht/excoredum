package io.justrade.gateway.http;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * A parsed inbound HTTP request: the method, the decoded path, query params,
 * the raw (JSON) body, request headers, and the path parameters captured by the
 * matching route. Value helpers throw {@link ApiException#badRequest} on a
 * missing or malformed numeric input.
 */
public final class HandlerRequest {

    private final FullHttpRequest request;
    private final HttpMethod method;
    private final QueryStringDecoder decoder;
    private final String path;
    private Map<String, String> pathParams = Map.of();

    HandlerRequest(final FullHttpRequest request) {
        this.request = request;
        this.method = request.method();
        this.decoder = new QueryStringDecoder(request.uri());
        this.path = decoder.path();
    }

    void pathParams(final Map<String, String> params) {
        this.pathParams = params;
    }

    public HttpMethod method() {
        return method;
    }

    public String path() {
        return path;
    }

    public String pathParam(final String name) {
        return pathParams.get(name);
    }

    public long pathLong(final String name) {
        return parseLong(pathParam(name), "path." + name);
    }

    public int pathInt(final String name) {
        return parseInt(pathParam(name), "path." + name);
    }

    public String header(final String name) {
        return request.headers().get(name);
    }

    /** The request body as a UTF-8 string (an empty body yields ""). */
    public String body() {
        final ByteBuf content = request.content();
        return content.toString(StandardCharsets.UTF_8);
    }

    public String queryParam(final String name) {
        final List<String> values = decoder.parameters().get(name);
        return values == null || values.isEmpty() ? null : values.get(0);
    }

    public int intParam(final String name) {
        return parseInt(queryParam(name), name);
    }

    public int intParam(final String name, final int defaultValue) {
        final String v = queryParam(name);
        return v == null ? defaultValue : parseInt(v, name);
    }

    public long longParam(final String name) {
        return parseLong(queryParam(name), name);
    }

    public long longParam(final String name, final long defaultValue) {
        final String v = queryParam(name);
        return v == null ? defaultValue : parseLong(v, name);
    }

    private static int parseInt(final String value, final String name) {
        try {
            return Integer.parseInt(value);
        } catch (final NumberFormatException | NullPointerException e) {
            throw ApiException.badRequest("invalid integer for " + name + ": " + value);
        }
    }

    private static long parseLong(final String value, final String name) {
        try {
            return Long.parseLong(value);
        } catch (final NumberFormatException | NullPointerException e) {
            throw ApiException.badRequest("invalid long for " + name + ": " + value);
        }
    }
}
