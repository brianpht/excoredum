package com.exadbe.gateway.http;

import com.exadbe.gateway.config.GatewayConfig;
import com.exadbe.gateway.config.SymbolRegistry;
import com.exadbe.gateway.dto.AddSymbolRequest;
import com.exadbe.gateway.dto.AddUserRequest;
import com.exadbe.gateway.dto.AdjustBalanceRequest;
import com.exadbe.gateway.dto.HealthDto;
import com.exadbe.gateway.dto.Mapper;
import com.exadbe.gateway.dto.ModifyOrderRequest;
import com.exadbe.gateway.dto.OrderBookRequest;
import com.exadbe.gateway.dto.PlaceOrderRequest;
import com.exadbe.gateway.read.ReadPump;
import com.exadbe.gateway.write.WritePump;
import com.exadbe.read.client.L2Snapshot;
import com.exadbe.read.client.MarketTradeResult;
import com.exadbe.read.client.OrderRecordResult;
import com.exadbe.read.client.TotalBalanceResult;
import com.exadbe.read.client.UserReport;
import io.netty.handler.codec.http.HttpMethod;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Maps HTTP method + path to a handler that turns the request into a
 * {@link CompletableFuture} carrying the JSON payload. Read endpoints bridge
 * {@link ReadPump}; write and admin endpoints bridge {@link WritePump}; the
 * symbol list and health are served directly. All handlers are boundary code
 * and never touch the engine.
 */
public final class Router {

    private final ReadPump read;
    private final WritePump write;
    private final SymbolRegistry symbols;
    private final GatewayConfig config;
    private final List<Route> routes = new ArrayList<>();

    public Router(final ReadPump read, final WritePump write, final GatewayConfig config) {
        this.read = read;
        this.write = write;
        this.config = config;
        this.symbols = new SymbolRegistry(config.symbols());
        register();
    }

    @FunctionalInterface
    private interface Handler {
        CompletableFuture<?> handle(HandlerRequest req);
    }

    private void route(final HttpMethod method, final String template, final Handler handler) {
        routes.add(new Route(method, new Template(template), handler));
    }

    /** Dispatches a matched request; throws {@link ApiException} synchronously for bad input / no route. */
    public CompletableFuture<?> route(final HandlerRequest req) {
        for (final Route r : routes) {
            if (!r.method().equals(req.method())) {
                continue;
            }
            final Map<String, String> params = r.template().match(req.path());
            if (params == null) {
                continue;
            }
            req.pathParams(params);
            return r.handler().handle(req);
        }
        throw ApiException.notFound("no route for " + req.method() + " " + req.path());
    }

    private void register() {
        // ---- read: market + portfolio + health ----
        route(HttpMethod.GET, "/api/v1/health", this::health);
        route(HttpMethod.GET, "/api/v1/symbols", req -> CompletableFuture.completedFuture(symbols.ordered()));
        route(HttpMethod.GET, "/api/v1/currencies", req -> CompletableFuture.completedFuture(config.currencies()));
        route(HttpMethod.GET, "/api/v1/orderbook", this::orderBook);
        route(HttpMethod.GET, "/api/v1/markettrades", this::marketTrades);
        route(HttpMethod.GET, "/api/v1/report/conservation", this::conservation);
        route(HttpMethod.GET, "/api/v1/users/{uid}/balances", this::userReport);
        route(HttpMethod.GET, "/api/v1/users/{uid}/orders", this::orderHistory);
        route(HttpMethod.GET, "/api/v1/users/{uid}/orders/active", this::activeOrders);
        route(HttpMethod.GET, "/api/v1/users/{uid}/trades", this::userTrades);
        route(HttpMethod.GET, "/api/v1/orders/{orderId}", this::orderById);

        // ---- write: trading ----
        route(HttpMethod.POST, "/api/v1/orders", this::placeOrder);
        route(HttpMethod.DELETE, "/api/v1/orders/{orderId}", this::cancelOrder);
        route(HttpMethod.PATCH, "/api/v1/orders/{orderId}", this::modifyOrder);
        route(HttpMethod.POST, "/api/v1/orderbook/{symbolId}/request", this::requestOrderBook);

        // ---- write: admin ----
        route(HttpMethod.POST, "/api/v1/symbols", this.admin(this::addSymbol));
        route(HttpMethod.POST, "/api/v1/users", this.admin(this::addUser));
        route(HttpMethod.POST, "/api/v1/users/{uid}/balance", this.admin(this::adjustBalance));
        route(HttpMethod.POST, "/api/v1/users/{uid}/suspend", this.admin(this::suspendUser));
        route(HttpMethod.POST, "/api/v1/users/{uid}/resume", this.admin(this::resumeUser));
    }

    // ---- health / symbols ----
    private CompletableFuture<Object> health(final HandlerRequest req) {
        final CompletableFuture<Object> hash = read.submitStateHash();
        final CompletableFuture<Object> totals = read.submitTotalCurrencyBalance();
        return hash.thenCombine(totals, (h, t) -> {
            final long applied = read.lastAppliedPosition();
            return new HealthDto(
                    applied,
                    (Long) h,
                    read.submitted(),
                    read.completed(),
                    read.expired(),
                    read.backpressure(),
                    // A replica that has applied nothing yet answers with
                    // empty/zero state; callers can distinguish it from a
                    // follower that genuinely holds that state.
                    applied > 0L,
                    Mapper.totalBalance((TotalBalanceResult) t).totals());
        });
    }

    // ---- read handlers ----
    private CompletableFuture<Object> orderBook(final HandlerRequest req) {
        final int symbolId = req.intParam("symbolId");
        final int maxLevels = req.intParam("maxLevels", 32);
        return read.submitOrderBook(symbolId, maxLevels).thenApply(v -> Mapper.orderBook((L2Snapshot) v));
    }

    private CompletableFuture<Object> marketTrades(final HandlerRequest req) {
        final int symbolId = req.intParam("symbolId");
        final int limit = req.intParam("limit", 100);
        return read.submitMarketTrades(symbolId, limit).thenApply(v -> trades((List<?>) v));
    }

    private CompletableFuture<Object> conservation(final HandlerRequest req) {
        return read.submitTotalCurrencyBalance().thenApply(v -> Mapper.totalBalance((TotalBalanceResult) v));
    }

    private CompletableFuture<Object> userReport(final HandlerRequest req) {
        final long uid = req.pathLong("uid");
        return read.submitSingleUserReport(uid).thenApply(v -> Mapper.userReport((UserReport) v));
    }

    private CompletableFuture<Object> orderHistory(final HandlerRequest req) {
        final long uid = req.pathLong("uid");
        return read.submitOrderHistory(uid).thenApply(v -> orders((List<?>) v));
    }

    private CompletableFuture<Object> activeOrders(final HandlerRequest req) {
        final long uid = req.pathLong("uid");
        return read.submitActiveOrders(uid).thenApply(v -> orders((List<?>) v));
    }

    private CompletableFuture<Object> userTrades(final HandlerRequest req) {
        final long uid = req.pathLong("uid");
        final int limit = req.intParam("limit", 100);
        return read.submitUserTrades(uid, limit).thenApply(v -> trades((List<?>) v));
    }

    private CompletableFuture<Object> orderById(final HandlerRequest req) {
        final long orderId = req.pathLong("orderId");
        return read.submitOrderById(orderId).thenApply(v -> {
            if (v == null) {
                throw ApiException.notFound("order not found: " + orderId);
            }
            return Mapper.order((OrderRecordResult) v);
        });
    }

    // ---- write handlers (trading) ----
    private CompletableFuture<?> placeOrder(final HandlerRequest req) {
        final PlaceOrderRequest body = readBody(req, PlaceOrderRequest.class);
        require(body.symbolId(), "symbolId");
        require(body.orderId(), "orderId");
        require(body.ask(), "ask");
        require(body.price(), "price");
        require(body.size(), "size");
        require(body.uid(), "uid");
        final String type = body.type() == null ? "GTC" : body.type().toUpperCase(Locale.ROOT);
        switch (type) {
            case "IOC":
                return write.placeIoc(
                        body.symbolId(),
                        body.orderId(),
                        body.ask(),
                        body.price(),
                        body.size(),
                        body.uid(),
                        body.userCookie());
            case "FOK_BUDGET":
                return write.placeFokBudget(
                        body.symbolId(),
                        body.orderId(),
                        body.ask(),
                        body.price(),
                        body.size(),
                        body.uid(),
                        body.userCookie());
            case "GTC":
                return write.placeGtc(
                        body.symbolId(),
                        body.orderId(),
                        body.ask(),
                        body.price(),
                        body.size(),
                        body.reserveBidPrice(),
                        body.uid(),
                        body.userCookie());
            default:
                throw ApiException.badRequest("unknown order type: " + type);
        }
    }

    private CompletableFuture<?> cancelOrder(final HandlerRequest req) {
        final long orderId = req.pathLong("orderId");
        final int symbolId = req.intParam("symbolId");
        final long uid = req.longParam("uid");
        return write.cancelOrder(symbolId, orderId, uid);
    }

    private CompletableFuture<?> modifyOrder(final HandlerRequest req) {
        final long orderId = req.pathLong("orderId");
        final ModifyOrderRequest body = readBody(req, ModifyOrderRequest.class);
        require(body.symbolId(), "symbolId");
        require(body.uid(), "uid");
        if (body.price() != null && body.size() != null) {
            throw ApiException.badRequest("PATCH order needs exactly one of 'price' or 'size'");
        }
        if (body.price() != null) {
            return write.moveOrder(body.symbolId(), orderId, body.price(), body.uid());
        }
        if (body.size() != null) {
            return write.reduceOrder(body.symbolId(), orderId, body.size(), body.uid());
        }
        throw ApiException.badRequest("PATCH order needs 'price' (move) or 'size' (reduce)");
    }

    private CompletableFuture<?> requestOrderBook(final HandlerRequest req) {
        final int symbolId = req.pathInt("symbolId");
        final OrderBookRequest body = readBody(req, OrderBookRequest.class);
        require(body.uid(), "uid");
        return write.requestOrderBook(symbolId, body.uid());
    }

    // ---- write handlers (admin) ----
    private CompletableFuture<?> addSymbol(final HandlerRequest req) {
        final AddSymbolRequest body = readBody(req, AddSymbolRequest.class);
        require(body.symbolId(), "symbolId");
        require(body.baseCurrency(), "baseCurrency");
        require(body.quoteCurrency(), "quoteCurrency");
        require(body.baseScaleK(), "baseScaleK");
        require(body.quoteScaleK(), "quoteScaleK");
        return write.addSymbol(
                body.symbolId(),
                body.baseCurrency(),
                body.quoteCurrency(),
                body.baseScaleK(),
                body.quoteScaleK(),
                body.takerFee(),
                body.makerFee());
    }

    private CompletableFuture<?> addUser(final HandlerRequest req) {
        final AddUserRequest body = readBody(req, AddUserRequest.class);
        require(body.uid(), "uid");
        return write.addUser(body.uid());
    }

    private CompletableFuture<?> adjustBalance(final HandlerRequest req) {
        final long uid = req.pathLong("uid");
        final AdjustBalanceRequest body = readBody(req, AdjustBalanceRequest.class);
        require(body.currency(), "currency");
        require(body.amount(), "amount");
        return write.adjustBalance(uid, body.currency(), body.amount());
    }

    private CompletableFuture<?> suspendUser(final HandlerRequest req) {
        return write.suspendUser(req.pathLong("uid"));
    }

    private CompletableFuture<?> resumeUser(final HandlerRequest req) {
        return write.resumeUser(req.pathLong("uid"));
    }

    // ---- helpers ----
    private Handler admin(final Handler delegate) {
        return req -> {
            checkAdmin(config, req);
            return delegate.handle(req);
        };
    }

    /**
     * Admin endpoints require an {@code X-User-Id} header in the configured
     * allow-list and, when an API key is configured, a matching {@code X-Api-Key}.
     */
    static void checkAdmin(final GatewayConfig config, final HandlerRequest req) {
        final String apiKey = config.adminApiKey();
        if (apiKey != null && !apiKey.isBlank()) {
            final String provided = req.header("X-Api-Key");
            if (provided == null || !constantTimeEquals(apiKey, provided)) {
                throw new ApiException(401, "invalid or missing X-Api-Key header for admin endpoint");
            }
        }
        final String uid = req.header("X-User-Id");
        if (uid == null) {
            throw ApiException.badRequest("missing X-User-Id header for admin endpoint");
        }
        final long parsed;
        try {
            parsed = Long.parseLong(uid);
        } catch (final NumberFormatException e) {
            throw ApiException.badRequest("invalid X-User-Id header: " + uid);
        }
        if (!config.adminUids().contains(parsed)) {
            throw new ApiException(403, "uid is not an admin: " + parsed);
        }
    }

    private static boolean constantTimeEquals(final String a, final String b) {
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    private static void require(final Object value, final String name) {
        if (value == null) {
            throw ApiException.badRequest("missing required field: " + name);
        }
    }

    private static <T> T readBody(final HandlerRequest req, final Class<T> type) {
        try {
            return Json.mapper().readValue(req.body(), type);
        } catch (final com.fasterxml.jackson.core.JsonProcessingException e) {
            throw ApiException.badRequest("invalid JSON body: " + e.getOriginalMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Object> orders(final List<?> records) {
        final List<Object> result = new ArrayList<>(records.size());
        for (final Object r : records) {
            result.add(Mapper.order((OrderRecordResult) r));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> trades(final List<?> trades) {
        final List<Object> result = new ArrayList<>(trades.size());
        for (final Object t : trades) {
            result.add(Mapper.trade((MarketTradeResult) t));
        }
        return result;
    }

    private record Route(HttpMethod method, Template template, Handler handler) {}

    /** Matches a path template with {@code {name}} placeholders against a request path. */
    private static final class Template {
        private final Pattern pattern;
        private final List<String> groups;

        Template(final String template) {
            final StringBuilder regex = new StringBuilder("^");
            final List<String> names = new ArrayList<>();
            for (final String segment : template.split("/")) {
                if (segment.isEmpty()) {
                    continue;
                }
                if (segment.startsWith("{") && segment.endsWith("}")) {
                    names.add(segment.substring(1, segment.length() - 1));
                    regex.append("/([^/]+)");
                } else {
                    regex.append('/').append(Pattern.quote(segment));
                }
            }
            if (template.endsWith("/")) {
                regex.append('/');
            }
            regex.append('$');
            this.pattern = Pattern.compile(regex.toString());
            this.groups = names;
        }

        Map<String, String> match(final String path) {
            final Matcher m = pattern.matcher(path);
            if (!m.matches()) {
                return null;
            }
            final Map<String, String> params = new HashMap<>();
            for (int i = 0; i < groups.size(); i++) {
                params.put(groups.get(i), m.group(i + 1));
            }
            return params;
        }
    }
}
