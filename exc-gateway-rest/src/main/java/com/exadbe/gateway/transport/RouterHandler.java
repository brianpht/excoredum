package com.exadbe.gateway.transport;

import com.exadbe.gateway.api.ApiErrorCodes;
import com.exadbe.gateway.codec.JsonReader;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.util.List;
import java.util.Map;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

/**
 * Netty inbound handler mapping HTTP requests onto pooled {@link GatewayRequest}
 * slots for the gateway agent. Event loops only route, parse path/query/body
 * fields, and enqueue; all registry lookups and cluster submission happen on
 * the agent thread. Requests are answered 400/404/503 here when they can be
 * rejected without agent involvement.
 */
public final class RouterHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final ManyToOneConcurrentArrayQueue<GatewayRequest> inbound;
    private final ManyToOneConcurrentArrayQueue<GatewayRequest> free;
    private final long requestTimeoutNs;
    private final JsonReader reader = new JsonReader();

    public RouterHandler(
            final ManyToOneConcurrentArrayQueue<GatewayRequest> inbound,
            final ManyToOneConcurrentArrayQueue<GatewayRequest> free,
            final long requestTimeoutNs) {
        this.inbound = inbound;
        this.free = free;
        this.requestTimeoutNs = requestTimeoutNs;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpRequest http) {
        final boolean keepAlive = HttpUtil.isKeepAlive(http);
        final QueryStringDecoder decoder = new QueryStringDecoder(http.uri());
        final String path = decoder.path();
        final HttpMethod method = http.method();

        final GatewayRequest slot = free.poll();
        if (slot == null) {
            HttpResponses.sendError(
                    ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, 0, "gateway request slots exhausted", keepAlive);
            return;
        }

        if (!route(slot, method, path)) {
            free.offer(slot);
            HttpResponses.sendError(
                    ctx,
                    HttpResponseStatus.NOT_FOUND,
                    ApiErrorCodes.UNKNOWN_ROUTE.gatewayErrorCode,
                    "unknown route: " + method + " " + path,
                    keepAlive);
            return;
        }

        slot.depth = parseDepth(decoder.parameters());
        slot.ctx = ctx;
        slot.keepAlive = keepAlive;
        slot.deadlineNanos = System.nanoTime() + requestTimeoutNs;

        final int bodyLength = http.content().readableBytes();
        if (bodyLength > 0 && !parseBody(slot, http, bodyLength)) {
            slot.reset();
            free.offer(slot);
            HttpResponses.sendError(
                    ctx,
                    HttpResponseStatus.BAD_REQUEST,
                    ApiErrorCodes.INVALID_BODY.gatewayErrorCode,
                    "malformed JSON body",
                    keepAlive);
            return;
        }

        if (!inbound.offer(slot)) {
            slot.reset();
            free.offer(slot);
            HttpResponses.sendError(
                    ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, 0, "gateway inbound queue full", keepAlive);
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        ctx.close();
    }

    /** Matches method and path onto a request kind and extracts string path parameters. */
    private static boolean route(final GatewayRequest slot, final HttpMethod method, final String path) {
        final String[] seg = path.split("/");
        if (seg.length >= 4 && "syncTradeApi".equals(seg[1]) && "v1".equals(seg[2])) {
            return routeTrade(slot, method, seg);
        }
        if (seg.length >= 4 && "syncAdminApi".equals(seg[1]) && "v1".equals(seg[2])) {
            return routeAdmin(slot, method, seg);
        }
        return false;
    }

    private static boolean routeTrade(final GatewayRequest slot, final HttpMethod method, final String[] seg) {
        switch (seg[3]) {
            case "ping":
                if (seg.length == 4 && HttpMethod.GET.equals(method)) {
                    slot.kind = GatewayRequest.PING;
                    return true;
                }
                return false;
            case "time":
                if (seg.length == 4 && HttpMethod.GET.equals(method)) {
                    slot.kind = GatewayRequest.TIME;
                    return true;
                }
                return false;
            case "info":
                if (seg.length == 4 && HttpMethod.GET.equals(method)) {
                    slot.kind = GatewayRequest.INFO;
                    return true;
                }
                return false;
            case "symbols":
                return routeTradeSymbols(slot, method, seg);
            case "users":
                if (seg.length == 6 && HttpMethod.GET.equals(method) && "state".equals(seg[5])) {
                    slot.kind = GatewayRequest.USER_STATE;
                    slot.uid = parseLongOrAbsent(seg[4]);
                    return slot.uid != GatewayRequest.ABSENT;
                }
                if (seg.length == 6 && HttpMethod.GET.equals(method) && "history".equals(seg[5])) {
                    slot.kind = GatewayRequest.USER_HISTORY;
                    slot.uid = parseLongOrAbsent(seg[4]);
                    return slot.uid != GatewayRequest.ABSENT;
                }
                return false;
            default:
                return false;
        }
    }

    private static boolean routeTradeSymbols(final GatewayRequest slot, final HttpMethod method, final String[] seg) {
        if (seg.length == 6 && HttpMethod.GET.equals(method) && "orderbook".equals(seg[5])) {
            slot.kind = GatewayRequest.ORDER_BOOK;
            slot.symbolCode = seg[4];
            return true;
        }
        if (seg.length == 8 && HttpMethod.POST.equals(method) && "trade".equals(seg[5]) && "orders".equals(seg[7])) {
            slot.kind = GatewayRequest.PLACE_ORDER;
            slot.symbolCode = seg[4];
            slot.uid = parseLongOrAbsent(seg[6]);
            return slot.uid != GatewayRequest.ABSENT;
        }
        if (seg.length == 9 && "trade".equals(seg[5]) && "orders".equals(seg[7])) {
            slot.symbolCode = seg[4];
            slot.uid = parseLongOrAbsent(seg[6]);
            slot.orderId = parseLongOrAbsent(seg[8]);
            if (slot.uid == GatewayRequest.ABSENT || slot.orderId == GatewayRequest.ABSENT) {
                return false;
            }
            if (HttpMethod.PUT.equals(method)) {
                slot.kind = GatewayRequest.MOVE_ORDER;
                return true;
            }
            if (HttpMethod.DELETE.equals(method)) {
                slot.kind = GatewayRequest.CANCEL_ORDER;
                return true;
            }
        }
        return false;
    }

    private static boolean routeAdmin(final GatewayRequest slot, final HttpMethod method, final String[] seg) {
        if (!HttpMethod.POST.equals(method)) {
            return false;
        }
        switch (seg[3]) {
            case "users":
                if (seg.length == 4) {
                    slot.kind = GatewayRequest.CREATE_USER;
                    return true;
                }
                if (seg.length == 6 && "accounts".equals(seg[5])) {
                    slot.kind = GatewayRequest.ADJUST_BALANCE;
                    slot.uid = parseLongOrAbsent(seg[4]);
                    return slot.uid != GatewayRequest.ABSENT;
                }
                return false;
            case "assets":
                if (seg.length == 4) {
                    slot.kind = GatewayRequest.CREATE_ASSET;
                    return true;
                }
                return false;
            case "symbols":
                if (seg.length == 4) {
                    slot.kind = GatewayRequest.CREATE_SYMBOL;
                    return true;
                }
                return false;
            default:
                return false;
        }
    }

    private static int parseDepth(final Map<String, List<String>> parameters) {
        final List<String> values = parameters.get("depth");
        if (values == null || values.isEmpty()) {
            return 0;
        }
        final long parsed = parseLongOrAbsent(values.get(0));
        return parsed == GatewayRequest.ABSENT || parsed > Integer.MAX_VALUE ? 0 : (int) parsed;
    }

    private boolean parseBody(final GatewayRequest slot, final FullHttpRequest http, final int bodyLength) {
        if (slot.bodyBuffer.length < bodyLength) {
            slot.bodyBuffer = new byte[Math.max(bodyLength, 1024)];
        }
        http.content().getBytes(http.content().readerIndex(), slot.bodyBuffer, 0, bodyLength);
        reader.wrap(slot.bodyBuffer, bodyLength);
        if (!reader.beginObject()) {
            return false;
        }
        while (reader.hasNextField()) {
            final String name = reader.fieldName();
            switch (name) {
                case "uid":
                    slot.uid = reader.nextLong();
                    break;
                case "transactionId":
                    slot.transactionId = reader.nextLong();
                    break;
                case "size":
                    slot.size = reader.nextLong();
                    break;
                case "userCookie":
                    slot.userCookie = reader.nextLong();
                    break;
                case "assetId":
                    slot.assetId = (int) reader.nextLong();
                    break;
                case "symbolId":
                    slot.symbolId = (int) reader.nextLong();
                    break;
                case "scale":
                    slot.scale = (int) reader.nextLong();
                    break;
                case "assetCode":
                    slot.assetCode = reader.nextString();
                    break;
                case "currency":
                    slot.currencyCode = reader.nextString();
                    break;
                case "symbolCode":
                    slot.symbolCode = reader.nextString();
                    break;
                case "symbolType":
                    slot.symbolType = reader.nextString();
                    break;
                case "baseAsset":
                    slot.baseAsset = reader.nextString();
                    break;
                case "quoteCurrency":
                    slot.quoteCurrency = reader.nextString();
                    break;
                case "action":
                    slot.action = reader.nextString();
                    break;
                case "orderType":
                    slot.orderType = reader.nextString();
                    break;
                case "price":
                    slot.priceText = reader.nextToken();
                    break;
                case "budget":
                    slot.budgetText = reader.nextToken();
                    break;
                case "amount":
                    slot.amountText = reader.nextToken();
                    break;
                case "lotSize":
                    slot.lotSizeText = reader.nextToken();
                    break;
                case "stepSize":
                    slot.stepSizeText = reader.nextToken();
                    break;
                case "takerFee":
                    slot.takerFeeText = reader.nextToken();
                    break;
                case "makerFee":
                    slot.makerFeeText = reader.nextToken();
                    break;
                case "marginBuy":
                    slot.marginBuyText = reader.nextToken();
                    break;
                case "marginSell":
                    slot.marginSellText = reader.nextToken();
                    break;
                default:
                    reader.skipValue();
                    break;
            }
            if (reader.failed()) {
                return false;
            }
        }
        return !reader.failed();
    }

    private static long parseLongOrAbsent(final String text) {
        if (text == null || text.isEmpty()) {
            return GatewayRequest.ABSENT;
        }
        long value = 0L;
        boolean negative = false;
        int i = 0;
        if (text.charAt(0) == '-') {
            negative = true;
            i = 1;
        }
        if (i == text.length()) {
            return GatewayRequest.ABSENT;
        }
        for (; i < text.length(); i++) {
            final char c = text.charAt(i);
            if (c < '0' || c > '9') {
                return GatewayRequest.ABSENT;
            }
            final int digit = c - '0';
            if (value > (Long.MAX_VALUE - digit) / 10L) {
                return GatewayRequest.ABSENT;
            }
            value = (value * 10L) + digit;
        }
        return negative ? -value : value;
    }
}
