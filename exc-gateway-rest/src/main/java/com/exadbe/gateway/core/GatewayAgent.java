package com.exadbe.gateway.core;

import com.exadbe.client.BackpressureException;
import com.exadbe.client.ExcClient;
import com.exadbe.client.ResultHandler;
import com.exadbe.config.CoreConfig;
import com.exadbe.engine.orderbook.L2View;
import com.exadbe.gateway.api.ApiErrorCodes;
import com.exadbe.gateway.codec.DecimalCodec;
import com.exadbe.gateway.codec.JsonWriter;
import com.exadbe.gateway.config.GatewayConfig;
import com.exadbe.gateway.transport.GatewayRequest;
import com.exadbe.gateway.transport.HttpResponses;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.read.ExcReadReplica;
import com.exadbe.read.report.SingleUserReport;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongLongConsumer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

/**
 * The single-writer heart of the gateway. One thread owns the cluster client,
 * the embedded read replica, and all gateway state: it polls both, drains
 * requests enqueued by the Netty event loops, submits commands, correlates
 * results by command id, sweeps request deadlines, and writes every HTTP
 * response. No locks are used anywhere in the gateway; cross-thread
 * communication is the two lock-free request queues and Netty's thread-safe
 * channel writes.
 *
 * <p>A 504 means the gateway gave up waiting for a response, not that the
 * command was lost: the client keeps retrying it idempotently until the
 * cluster acknowledges it.
 */
public final class GatewayAgent implements Runnable, ResultHandler {

    private static final int INBOUND_BATCH = 64;
    private static final int MAX_TRACKED_TAKER_ORDERS = 4096;
    private static final String SYMBOL_TYPE_SPOT = "CURRENCY_EXCHANGE_PAIR";

    private final ExcReadReplica replica;
    private final GatewayState state;
    private final ManyToOneConcurrentArrayQueue<GatewayRequest> inbound;
    private final ManyToOneConcurrentArrayQueue<GatewayRequest> free;
    private final Long2ObjectHashMap<GatewayRequest> inFlight;
    private final Long2ObjectHashMap<GatewayOrder> takerOrderByCommand = new Long2ObjectHashMap<>(256, 0.65f);
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final JsonWriter json = new JsonWriter(8192);
    private final L2View l2View = new L2View(CoreConfig.DEFAULT_L2_MAX_LEVELS);
    private final long[] parseOut = new long[1];
    private final LongLongConsumer balanceWriter = this::writeBalance;
    private final long orderIdPrefix;
    private long orderSeq = 1L;
    private GatewayRequest inFlightHead;
    private GatewayRequest inFlightTail;
    private ExcClient client;
    private volatile boolean running = true;

    public GatewayAgent(
            final GatewayConfig config,
            final ExcReadReplica replica,
            final GatewayState state,
            final ManyToOneConcurrentArrayQueue<GatewayRequest> inbound,
            final ManyToOneConcurrentArrayQueue<GatewayRequest> free) {
        this.replica = replica;
        this.state = state;
        this.inbound = inbound;
        this.free = free;
        this.inFlight = new Long2ObjectHashMap<>(Math.max(16, config.maxInFlight() * 2), 0.65f);
        this.orderIdPrefix = (long) config.gatewayId() << 48;
    }

    /** Attaches the cluster client; must run before the agent thread starts. */
    public void bind(final ExcClient excClient) {
        this.client = excClient;
        excClient.tradeListener(this::onTrade);
        excClient.reduceListener(this::onReduce);
        excClient.rejectListener(this::onReject);
    }

    @Override
    public void run() {
        while (running) {
            int work = 0;
            work += client.poll();
            work += replica.poll();
            work += drainInbound();
            work += sweepExpired();
            idleStrategy.idle(work);
        }
    }

    /** Signals the agent loop to exit; the thread drains its current iteration first. */
    public void stop() {
        running = false;
    }

    private int drainInbound() {
        int count = 0;
        for (int i = 0; i < INBOUND_BATCH; i++) {
            final GatewayRequest req = inbound.poll();
            if (req == null) {
                break;
            }
            count++;
            try {
                dispatch(req);
            } catch (final BackpressureException e) {
                sendError(req, HttpResponseStatus.SERVICE_UNAVAILABLE, 0, "in-flight window full, retry later");
                release(req);
            }
        }
        return count;
    }

    private void dispatch(final GatewayRequest req) {
        switch (req.kind) {
            case GatewayRequest.PING:
                handlePing(req);
                break;
            case GatewayRequest.TIME:
                handleTime(req);
                break;
            case GatewayRequest.INFO:
                handleInfo(req);
                break;
            case GatewayRequest.ORDER_BOOK:
                handleOrderBook(req);
                break;
            case GatewayRequest.USER_STATE:
                handleUserState(req);
                break;
            case GatewayRequest.USER_HISTORY:
                handleUserHistory(req);
                break;
            case GatewayRequest.CREATE_USER:
                handleCreateUser(req);
                break;
            case GatewayRequest.ADJUST_BALANCE:
                handleAdjustBalance(req);
                break;
            case GatewayRequest.CREATE_ASSET:
                handleCreateAsset(req);
                break;
            case GatewayRequest.CREATE_SYMBOL:
                handleCreateSymbol(req);
                break;
            case GatewayRequest.PLACE_ORDER:
                handlePlaceOrder(req);
                break;
            case GatewayRequest.MOVE_ORDER:
                handleMoveOrder(req);
                break;
            case GatewayRequest.CANCEL_ORDER:
                handleCancelOrder(req);
                break;
            default:
                sendError(req, HttpResponseStatus.NOT_FOUND, ApiErrorCodes.UNKNOWN_ROUTE, null);
                release(req);
                break;
        }
    }

    // ------------------------------------------------------------------
    // Read handlers (served from the embedded replica or gateway registry)
    // ------------------------------------------------------------------

    private void handlePing(final GatewayRequest req) {
        beginEnvelope(0, 0, "OK");
        json.valueNull().endObject();
        send(req, HttpResponseStatus.OK);
        release(req);
    }

    private void handleTime(final GatewayRequest req) {
        final long nowMillis = System.currentTimeMillis();
        beginEnvelope(0, 0, "OK");
        writeTimeObject(nowMillis);
        json.endObject();
        send(req, HttpResponseStatus.OK);
        release(req);
    }

    private void handleInfo(final GatewayRequest req) {
        beginEnvelope(0, 0, "OK");
        json.beginObject().name("serverTime");
        writeTimeObject(System.currentTimeMillis());
        json.name("assets").beginArray();
        for (final GatewayAssetSpec asset : state.assets()) {
            if (!asset.active()) {
                continue;
            }
            json.beginObject()
                    .name("code")
                    .valueString(asset.assetCode())
                    .name("scale")
                    .valueLong(asset.scale())
                    .endObject();
        }
        json.endArray().name("symbols").beginArray();
        for (final GatewaySymbolSpec symbol : state.symbols()) {
            if (symbol.status() != GatewaySymbolSpec.STATUS_ACTIVE) {
                continue;
            }
            writeSymbolSpec(symbol);
        }
        json.endArray().endObject().endObject();
        send(req, HttpResponseStatus.OK);
        release(req);
    }

    private void handleOrderBook(final GatewayRequest req) {
        final GatewaySymbolSpec spec = state.getSymbolSpec(req.symbolCode);
        if (spec == null) {
            sendError(req, HttpResponseStatus.NOT_FOUND, ApiErrorCodes.UNKNOWN_SYMBOL_404, null);
            release(req);
            return;
        }
        if (!replica.orderBook(spec.symbolId(), l2View)) {
            sendError(
                    req,
                    HttpResponseStatus.NOT_FOUND,
                    ApiErrorCodes.UNKNOWN_SYMBOL_404,
                    "replica has not replicated this symbol yet");
            release(req);
            return;
        }
        final int depth = req.depth <= 0 ? l2View.maxLevels() : Math.min(req.depth, l2View.maxLevels());
        final int quoteScale = spec.quoteCurrency().scale();
        beginEnvelope(0, 0, "OK");
        json.beginObject()
                .name("symbol")
                .valueString(spec.symbolCode())
                .name("askPrices")
                .beginArray();
        final int asks = Math.min(l2View.askDepth(), depth);
        for (int i = 0; i < asks; i++) {
            json.valueDecimal(l2View.askPrice(i), quoteScale);
        }
        json.endArray().name("askVolumes").beginArray();
        for (int i = 0; i < asks; i++) {
            json.valueLong(l2View.askVolume(i));
        }
        json.endArray().name("bidPrices").beginArray();
        final int bids = Math.min(l2View.bidDepth(), depth);
        for (int i = 0; i < bids; i++) {
            json.valueDecimal(l2View.bidPrice(i), quoteScale);
        }
        json.endArray().name("bidVolumes").beginArray();
        for (int i = 0; i < bids; i++) {
            json.valueLong(l2View.bidVolume(i));
        }
        json.endArray().endObject().endObject();
        send(req, HttpResponseStatus.OK);
        release(req);
    }

    private void handleUserState(final GatewayRequest req) {
        final SingleUserReport report = replica.singleUserReport(req.uid);
        if (!report.exists()) {
            sendError(req, HttpResponseStatus.NOT_FOUND, ApiErrorCodes.UNKNOWN_USER_404, null);
            release(req);
            return;
        }
        beginEnvelope(0, 0, "OK");
        json.beginObject().name("uid").valueLong(req.uid).name("accounts").beginArray();
        report.forEachBalance(balanceWriter);
        json.endArray().name("activeOrders").beginArray();
        final GatewayUserProfile profile = state.getUserProfile(req.uid);
        for (final SingleUserReport.OrderLine line : report.orders()) {
            writeReportOrderLine(line, profile);
        }
        json.endArray().endObject().endObject();
        send(req, HttpResponseStatus.OK);
        release(req);
    }

    private void writeBalance(final long currencyId, final long balance) {
        final GatewayAssetSpec asset = state.getAssetSpec((int) currencyId);
        json.beginObject()
                .name("currency")
                .valueString(asset == null ? String.valueOf(currencyId) : asset.assetCode())
                .name("balance")
                .valueDecimal(balance, asset == null ? 0 : asset.scale())
                .endObject();
    }

    private void writeReportOrderLine(final SingleUserReport.OrderLine line, final GatewayUserProfile profile) {
        final GatewaySymbolSpec spec = state.getSymbolSpec(line.symbolId());
        final int priceScale = spec == null ? 0 : spec.quoteCurrency().scale();
        long userCookie = 0L;
        String orderType = "GTC";
        if (profile != null) {
            final GatewayOrder tracked = profile.order(line.orderId());
            if (tracked != null) {
                userCookie = tracked.userCookie();
                orderType = tracked.orderType();
            }
        }
        json.beginObject()
                .name("orderId")
                .valueLong(line.orderId())
                .name("price")
                .valueDecimal(line.price(), priceScale)
                .name("size")
                .valueLong(line.size())
                .name("filled")
                .valueLong(line.filled())
                .name("state")
                .valueString("ACTIVE")
                .name("userCookie")
                .valueLong(userCookie)
                .name("action")
                .valueString(line.ask() ? "ASK" : "BID")
                .name("orderType")
                .valueString(orderType)
                .name("symbol")
                .valueString(spec == null ? String.valueOf(line.symbolId()) : spec.symbolCode())
                .name("deals")
                .beginArray()
                .endArray()
                .endObject();
    }

    private void handleUserHistory(final GatewayRequest req) {
        final GatewayUserProfile profile = state.getUserProfile(req.uid);
        if (profile == null) {
            sendError(req, HttpResponseStatus.NOT_FOUND, ApiErrorCodes.UNKNOWN_USER_404, null);
            release(req);
            return;
        }
        beginEnvelope(0, 0, "OK");
        json.beginObject().name("uid").valueLong(req.uid).name("orders").beginArray();
        for (final GatewayOrder order : profile.ordersMap().values()) {
            writeTrackedOrder(order);
        }
        json.endArray().endObject().endObject();
        send(req, HttpResponseStatus.OK);
        release(req);
    }

    private void writeTrackedOrder(final GatewayOrder order) {
        final GatewaySymbolSpec spec = state.getSymbolSpec(order.symbolId());
        final int priceScale = spec == null ? 0 : spec.quoteCurrency().scale();
        json.beginObject()
                .name("orderId")
                .valueLong(order.orderId())
                .name("price")
                .valueDecimal(order.price(), priceScale)
                .name("size")
                .valueLong(order.size())
                .name("filled")
                .valueLong(order.filled())
                .name("state")
                .valueString(order.stateName())
                .name("userCookie")
                .valueLong(order.userCookie())
                .name("action")
                .valueString(order.ask() ? "ASK" : "BID")
                .name("orderType")
                .valueString(order.orderType())
                .name("symbol")
                .valueString(order.symbolCode())
                .name("deals")
                .beginArray();
        for (final GatewayOrder.Deal deal : order.deals()) {
            json.beginObject()
                    .name("party")
                    .valueString(deal.taker() ? "TAKER" : "MAKER")
                    .name("price")
                    .valueDecimal(deal.price(), priceScale)
                    .name("size")
                    .valueLong(deal.size())
                    .endObject();
        }
        json.endArray().endObject();
    }

    // ------------------------------------------------------------------
    // Admin handlers
    // ------------------------------------------------------------------

    private void handleCreateUser(final GatewayRequest req) {
        if (req.uid == GatewayRequest.ABSENT) {
            sendError(req, HttpResponseStatus.BAD_REQUEST, ApiErrorCodes.INVALID_BODY, "uid is required");
            release(req);
            return;
        }
        submitAndTrack(req, client.addUser(req.uid));
    }

    private void handleAdjustBalance(final GatewayRequest req) {
        if (req.uid == GatewayRequest.ABSENT || req.currencyCode == null || req.amountText == null) {
            sendError(
                    req,
                    HttpResponseStatus.BAD_REQUEST,
                    ApiErrorCodes.INVALID_BODY,
                    "uid, currency and amount are required");
            release(req);
            return;
        }
        final GatewayAssetSpec asset = state.getAssetSpec(req.currencyCode);
        if (asset == null) {
            sendError(req, HttpResponseStatus.BAD_REQUEST, ApiErrorCodes.UNKNOWN_CURRENCY, null);
            release(req);
            return;
        }
        final int status = DecimalCodec.parseScaled(req.amountText, asset.scale(), parseOut);
        if (status == DecimalCodec.PRECISION_TOO_HIGH) {
            sendError(req, HttpResponseStatus.BAD_REQUEST, ApiErrorCodes.PRECISION_IS_TOO_HIGH, null);
            release(req);
            return;
        }
        if (status != DecimalCodec.OK) {
            sendError(req, HttpResponseStatus.BAD_REQUEST, ApiErrorCodes.INVALID_BODY, "amount is not a valid decimal");
            release(req);
            return;
        }
        req.amount = parseOut[0];
        submitAndTrack(req, client.adjustBalance(req.uid, asset.assetId(), req.amount));
    }

    private void handleCreateAsset(final GatewayRequest req) {
        if (req.assetCode == null || req.assetId < 0 || req.scale < 0) {
            sendError(
                    req,
                    HttpResponseStatus.BAD_REQUEST,
                    ApiErrorCodes.INVALID_BODY,
                    "assetCode, assetId and scale are required");
            release(req);
            return;
        }
        final GatewayAssetSpec spec = new GatewayAssetSpec(req.assetCode, req.assetId, req.scale, true);
        if (!state.registerNewAsset(spec)) {
            sendError(req, HttpResponseStatus.BAD_REQUEST, ApiErrorCodes.ASSET_ALREADY_EXISTS, null);
            release(req);
            return;
        }
        beginEnvelope(0, 0, "OK");
        json.beginObject()
                .name("assetCode")
                .valueString(spec.assetCode())
                .name("assetId")
                .valueLong(spec.assetId())
                .name("scale")
                .valueLong(spec.scale())
                .endObject()
                .endObject();
        send(req, HttpResponseStatus.CREATED);
        release(req);
    }

    private void handleCreateSymbol(final GatewayRequest req) {
        if (req.symbolCode == null || req.symbolId < 0) {
            sendError(
                    req,
                    HttpResponseStatus.BAD_REQUEST,
                    ApiErrorCodes.INVALID_BODY,
                    "symbolCode and symbolId are required");
            release(req);
            return;
        }
        if (req.symbolType != null && !SYMBOL_TYPE_SPOT.equals(req.symbolType)) {
            sendError(
                    req,
                    HttpResponseStatus.BAD_REQUEST,
                    ApiErrorCodes.INVALID_CONFIGURATION,
                    "only CURRENCY_EXCHANGE_PAIR (spot) is supported");
            release(req);
            return;
        }
        final GatewayAssetSpec base = state.getAssetSpec(req.baseAsset);
        if (base == null) {
            sendError(req, HttpResponseStatus.BAD_REQUEST, ApiErrorCodes.UNKNOWN_BASE_ASSET, null);
            release(req);
            return;
        }
        final GatewayAssetSpec quote = state.getAssetSpec(req.quoteCurrency);
        if (quote == null) {
            sendError(req, HttpResponseStatus.BAD_REQUEST, ApiErrorCodes.UNKNOWN_QUOTE_CURRENCY, null);
            release(req);
            return;
        }
        if (req.lotSizeText == null
                || parseScaled(req.lotSizeText, base.scale()) != DecimalCodec.OK
                || parseOut[0] <= 0L) {
            sendError(
                    req,
                    HttpResponseStatus.BAD_REQUEST,
                    ApiErrorCodes.INVALID_CONFIGURATION,
                    "lot size must be a positive integer in base asset units");
            release(req);
            return;
        }
        final long lotSize = parseOut[0];
        if (req.stepSizeText == null
                || parseScaled(req.stepSizeText, quote.scale()) != DecimalCodec.OK
                || parseOut[0] <= 0L) {
            sendError(
                    req,
                    HttpResponseStatus.BAD_REQUEST,
                    ApiErrorCodes.INVALID_CONFIGURATION,
                    "step size must be a positive integer in quote currency units");
            release(req);
            return;
        }
        final long stepSize = parseOut[0];
        final long takerFee;
        if (req.takerFeeText == null) {
            takerFee = 0L;
        } else if (parseScaled(req.takerFeeText, quote.scale()) != DecimalCodec.OK || parseOut[0] < 0L) {
            sendError(
                    req,
                    HttpResponseStatus.BAD_REQUEST,
                    ApiErrorCodes.INVALID_CONFIGURATION,
                    "taker fee must be a non-negative integer in quote currency units");
            release(req);
            return;
        } else {
            takerFee = parseOut[0];
        }
        final long makerFee;
        if (req.makerFeeText == null) {
            makerFee = 0L;
        } else if (parseScaled(req.makerFeeText, quote.scale()) != DecimalCodec.OK || parseOut[0] < 0L) {
            sendError(
                    req,
                    HttpResponseStatus.BAD_REQUEST,
                    ApiErrorCodes.INVALID_CONFIGURATION,
                    "maker fee must be a non-negative integer in quote currency units");
            release(req);
            return;
        } else {
            makerFee = parseOut[0];
        }
        if (takerFee < makerFee) {
            sendError(
                    req,
                    HttpResponseStatus.BAD_REQUEST,
                    ApiErrorCodes.INVALID_CONFIGURATION,
                    "taker fee can not be less than maker fee");
            release(req);
            return;
        }
        if (!marginIsZero(req.marginBuyText, quote.scale()) || !marginIsZero(req.marginSellText, quote.scale())) {
            sendError(
                    req,
                    HttpResponseStatus.BAD_REQUEST,
                    ApiErrorCodes.INVALID_CONFIGURATION,
                    "margin must be zero in exchange mode");
            release(req);
            return;
        }
        final GatewaySymbolSpec spec = new GatewaySymbolSpec(
                req.symbolId,
                req.symbolCode,
                req.symbolType == null ? SYMBOL_TYPE_SPOT : req.symbolType,
                base,
                quote,
                lotSize,
                stepSize,
                takerFee,
                makerFee,
                GatewaySymbolSpec.STATUS_NEW);
        if (!state.registerNewSymbol(spec)) {
            sendError(req, HttpResponseStatus.BAD_REQUEST, ApiErrorCodes.SYMBOL_ALREADY_EXISTS, null);
            release(req);
            return;
        }
        submitAndTrack(
                req,
                client.addSymbol(req.symbolId, base.assetId(), quote.assetId(), lotSize, stepSize, takerFee, makerFee));
    }

    private boolean marginIsZero(final String text, final int scale) {
        if (text == null) {
            return true;
        }
        return parseScaled(text, scale) == DecimalCodec.OK && parseOut[0] == 0L;
    }

    // ------------------------------------------------------------------
    // Trade handlers
    // ------------------------------------------------------------------

    private void handlePlaceOrder(final GatewayRequest req) {
        final GatewaySymbolSpec spec = state.getSymbolSpec(req.symbolCode);
        if (spec == null) {
            sendError(req, HttpResponseStatus.NOT_FOUND, ApiErrorCodes.UNKNOWN_SYMBOL_404, null);
            release(req);
            return;
        }
        final boolean ask;
        if ("ASK".equals(req.action)) {
            ask = true;
        } else if ("BID".equals(req.action)) {
            ask = false;
        } else {
            sendError(
                    req,
                    HttpResponseStatus.BAD_REQUEST,
                    ApiErrorCodes.INVALID_CONFIGURATION,
                    "action must be ASK or BID");
            release(req);
            return;
        }
        final String orderType = req.orderType == null ? "GTC" : req.orderType;
        if (req.size == GatewayRequest.ABSENT || req.size <= 0L) {
            sendError(req, HttpResponseStatus.BAD_REQUEST, ApiErrorCodes.INVALID_SIZE, null);
            release(req);
            return;
        }
        final int quoteScale = spec.quoteCurrency().scale();
        final long orderId = orderIdPrefix | orderSeq++;
        final long userCookie = req.userCookie == GatewayRequest.ABSENT ? 0L : req.userCookie;
        final long commandIdLo;
        switch (orderType) {
            case "GTC":
                if (!parseNonNegative(req.priceText, quoteScale)) {
                    sendError(req, HttpResponseStatus.BAD_REQUEST, ApiErrorCodes.INVALID_PRICE, null);
                    release(req);
                    return;
                }
                req.price = parseOut[0];
                commandIdLo = client.placeGtc(spec.symbolId(), orderId, ask, req.price, req.size, req.price, req.uid);
                break;
            case "IOC":
                if (!parseNonNegative(req.priceText, quoteScale)) {
                    sendError(req, HttpResponseStatus.BAD_REQUEST, ApiErrorCodes.INVALID_PRICE, null);
                    release(req);
                    return;
                }
                req.price = parseOut[0];
                commandIdLo = client.placeIoc(spec.symbolId(), orderId, ask, req.price, req.size, req.uid);
                break;
            case "FOK_BUDGET":
                if (!parseNonNegative(req.budgetText, quoteScale)) {
                    sendError(req, HttpResponseStatus.BAD_REQUEST, ApiErrorCodes.INVALID_PRICE, null);
                    release(req);
                    return;
                }
                req.budget = parseOut[0];
                commandIdLo = client.placeFokBudget(spec.symbolId(), orderId, ask, req.budget, req.size, req.uid);
                break;
            default:
                sendError(
                        req,
                        HttpResponseStatus.BAD_REQUEST,
                        ApiErrorCodes.INVALID_CONFIGURATION,
                        "orderType must be GTC, IOC or FOK_BUDGET");
                release(req);
                return;
        }
        final long displayPrice = req.price != GatewayRequest.ABSENT ? req.price : req.budget;
        state.getOrCreateUserProfile(req.uid)
                .addOrder(new GatewayOrder(
                        orderId,
                        spec.symbolCode(),
                        spec.symbolId(),
                        ask,
                        orderType,
                        userCookie,
                        displayPrice,
                        req.size,
                        GatewayOrder.STATE_NEW,
                        commandIdLo));
        req.orderId = orderId;
        submitAndTrack(req, commandIdLo);
    }

    private void handleMoveOrder(final GatewayRequest req) {
        final GatewaySymbolSpec spec = state.getSymbolSpec(req.symbolCode);
        if (spec == null) {
            sendError(req, HttpResponseStatus.NOT_FOUND, ApiErrorCodes.UNKNOWN_SYMBOL_404, null);
            release(req);
            return;
        }
        if (!parseNonNegative(req.priceText, spec.quoteCurrency().scale())) {
            sendError(req, HttpResponseStatus.BAD_REQUEST, ApiErrorCodes.INVALID_PRICE, null);
            release(req);
            return;
        }
        req.price = parseOut[0];
        submitAndTrack(req, client.moveOrder(spec.symbolId(), req.orderId, req.price, req.uid));
    }

    private void handleCancelOrder(final GatewayRequest req) {
        final GatewaySymbolSpec spec = state.getSymbolSpec(req.symbolCode);
        if (spec == null) {
            sendError(req, HttpResponseStatus.NOT_FOUND, ApiErrorCodes.UNKNOWN_SYMBOL_404, null);
            release(req);
            return;
        }
        submitAndTrack(req, client.cancelOrder(spec.symbolId(), req.orderId, req.uid));
    }

    private boolean parseNonNegative(final String text, final int scale) {
        return text != null && parseScaled(text, scale) == DecimalCodec.OK && parseOut[0] >= 0L;
    }

    private int parseScaled(final String text, final int scale) {
        return DecimalCodec.parseScaled(text, scale, parseOut);
    }

    // ------------------------------------------------------------------
    // Submission, correlation, and result delivery
    // ------------------------------------------------------------------

    private void submitAndTrack(final GatewayRequest req, final long commandIdLo) {
        req.commandIdLo = commandIdLo;
        inFlight.put(commandIdLo, req);
        linkInFlight(req);
    }

    @Override
    public void onResult(
            final long commandIdHi,
            final long commandIdLo,
            final CommandResultCode resultCode,
            final long uid,
            final boolean hasUid,
            final long orderId,
            final boolean hasOrderId,
            final long filledSize,
            final boolean hasFilledSize) {
        final GatewayRequest req = inFlight.remove(commandIdLo);
        if (req == null) {
            return;
        }
        unlinkInFlight(req);
        final boolean success = resultCode == CommandResultCode.SUCCESS;
        applyResultSideEffects(req, success, filledSize, hasFilledSize);
        beginEnvelope(0, resultCode.value(), resultCode.name());
        writeResultData(req);
        json.endObject();
        send(req, success ? successStatus(req.kind) : HttpResponseStatus.BAD_REQUEST);
        release(req);
    }

    @Override
    public void onExpired(final long commandIdHi, final long commandIdLo) {
        final GatewayRequest req = inFlight.remove(commandIdLo);
        if (req == null) {
            return;
        }
        unlinkInFlight(req);
        beginEnvelope(0, 0, "command expired after retries");
        json.valueNull().endObject();
        send(req, HttpResponseStatus.GATEWAY_TIMEOUT);
        release(req);
    }

    private void applyResultSideEffects(
            final GatewayRequest req, final boolean success, final long filledSize, final boolean hasFilledSize) {
        switch (req.kind) {
            case GatewayRequest.CREATE_SYMBOL:
                if (success) {
                    state.activateSymbol(req.symbolId);
                } else {
                    state.removeSymbol(req.symbolId);
                }
                break;
            case GatewayRequest.PLACE_ORDER: {
                final GatewayUserProfile profile = state.getUserProfile(req.uid);
                final GatewayOrder order = profile == null ? null : profile.order(req.orderId);
                if (order == null) {
                    break;
                }
                if (success) {
                    order.state(GatewayOrder.STATE_ACTIVE);
                    if (hasFilledSize && filledSize >= order.size()) {
                        order.state(GatewayOrder.STATE_COMPLETED);
                    }
                    // Track even an instantly completed order: its trade frames
                    // trail the result and carry the deals for history.
                    trackTaker(req.commandIdLo, order);
                } else {
                    profile.removeOrder(req.orderId);
                }
                break;
            }
            case GatewayRequest.MOVE_ORDER: {
                if (!success) {
                    break;
                }
                final GatewayUserProfile profile = state.getUserProfile(req.uid);
                final GatewayOrder order = profile == null ? null : profile.order(req.orderId);
                if (order != null) {
                    order.price(req.price);
                }
                break;
            }
            case GatewayRequest.CANCEL_ORDER: {
                if (!success) {
                    break;
                }
                final GatewayUserProfile profile = state.getUserProfile(req.uid);
                final GatewayOrder order = profile == null ? null : profile.order(req.orderId);
                if (order != null) {
                    order.state(GatewayOrder.STATE_CANCELLED);
                    takerOrderByCommand.remove(order.commandIdLo());
                }
                break;
            }
            default:
                break;
        }
    }

    private void writeResultData(final GatewayRequest req) {
        switch (req.kind) {
            case GatewayRequest.CREATE_USER:
            case GatewayRequest.ADJUST_BALANCE:
                json.valueLong(req.uid);
                break;
            case GatewayRequest.CREATE_SYMBOL:
                writeSymbolSpec(state.getSymbolSpec(req.symbolId));
                break;
            case GatewayRequest.PLACE_ORDER:
            case GatewayRequest.MOVE_ORDER:
            case GatewayRequest.CANCEL_ORDER:
                writeOrderResult(req);
                break;
            default:
                json.valueNull();
                break;
        }
    }

    private void writeOrderResult(final GatewayRequest req) {
        final GatewayUserProfile profile = state.getUserProfile(req.uid);
        final GatewayOrder order = profile == null ? null : profile.order(req.orderId);
        final String stateName =
                order != null ? order.stateName() : (req.kind == GatewayRequest.CANCEL_ORDER ? "CANCELLED" : "NEW");
        json.beginObject()
                .name("orderId")
                .valueLong(req.orderId)
                .name("size")
                .valueLong(req.kind == GatewayRequest.PLACE_ORDER ? req.size : -1L)
                .name("filled")
                .valueLong(req.kind == GatewayRequest.PLACE_ORDER ? 0L : -1L)
                .name("state")
                .valueString(stateName)
                .name("userCookie")
                .valueLong(order == null ? 0L : order.userCookie())
                .name("action")
                .valueString(order == null ? null : (order.ask() ? "ASK" : "BID"))
                .name("orderType")
                .valueString(order == null ? null : order.orderType())
                .name("symbol")
                .valueString(req.symbolCode)
                .name("deals")
                .beginArray()
                .endArray()
                .endObject();
    }

    private void writeSymbolSpec(final GatewaySymbolSpec spec) {
        if (spec == null) {
            json.valueNull();
            return;
        }
        json.beginObject()
                .name("symbolId")
                .valueLong(spec.symbolId())
                .name("symbolCode")
                .valueString(spec.symbolCode())
                .name("symbolType")
                .valueString(spec.symbolType())
                .name("baseAsset")
                .valueString(spec.baseAsset().assetCode())
                .name("quoteCurrency")
                .valueString(spec.quoteCurrency().assetCode())
                .name("lotSize")
                .valueDecimal(spec.baseScaleK(), spec.baseAsset().scale())
                .name("stepSize")
                .valueDecimal(spec.quoteScaleK(), spec.quoteCurrency().scale())
                .name("takerFee")
                .valueDecimal(spec.takerFee(), spec.quoteCurrency().scale())
                .name("makerFee")
                .valueDecimal(spec.makerFee(), spec.quoteCurrency().scale())
                .name("status")
                .valueString(spec.status() == GatewaySymbolSpec.STATUS_ACTIVE ? "ACTIVE" : "NEW")
                .endObject();
    }

    private void writeTimeObject(final long nowMillis) {
        json.beginObject()
                .name("isoTime")
                .valueString(DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(nowMillis)))
                .name("epoch")
                .valueLong(nowMillis)
                .endObject();
    }

    private HttpResponseStatus successStatus(final int kind) {
        switch (kind) {
            case GatewayRequest.CREATE_USER:
            case GatewayRequest.ADJUST_BALANCE:
            case GatewayRequest.CREATE_ASSET:
            case GatewayRequest.CREATE_SYMBOL:
            case GatewayRequest.PLACE_ORDER:
                return HttpResponseStatus.CREATED;
            default:
                return HttpResponseStatus.OK;
        }
    }

    // ------------------------------------------------------------------
    // Egress event listeners (profile tracking)
    // ------------------------------------------------------------------

    private void onTrade(
            final long commandIdHi,
            final long commandIdLo,
            final int eventIndex,
            final int symbolId,
            final long makerOrderId,
            final long makerUid,
            final long takerUid,
            final long price,
            final long size,
            final boolean makerCompleted) {
        final GatewayUserProfile maker = state.getUserProfile(makerUid);
        if (maker != null) {
            final GatewayOrder order = maker.order(makerOrderId);
            if (order != null) {
                order.addDeal(false, price, size);
                if (makerCompleted) {
                    order.state(GatewayOrder.STATE_COMPLETED);
                    takerOrderByCommand.remove(order.commandIdLo());
                }
            }
        }
        final GatewayOrder takerOrder = takerOrderByCommand.get(commandIdLo);
        if (takerOrder != null) {
            takerOrder.addDeal(true, price, size);
        }
    }

    private void onReduce(
            final long commandIdHi,
            final long commandIdLo,
            final int eventIndex,
            final int symbolId,
            final long orderId,
            final long uid,
            final long reducedBy,
            final long price,
            final boolean orderCompleted) {
        final GatewayUserProfile profile = state.getUserProfile(uid);
        if (profile == null) {
            return;
        }
        final GatewayOrder order = profile.order(orderId);
        if (order == null) {
            return;
        }
        if (orderCompleted) {
            if (order.state() == GatewayOrder.STATE_ACTIVE || order.state() == GatewayOrder.STATE_NEW) {
                order.state(GatewayOrder.STATE_CANCELLED);
            }
            takerOrderByCommand.remove(order.commandIdLo());
        } else {
            order.size(order.size() - reducedBy);
        }
    }

    private void onReject(
            final long commandIdHi,
            final long commandIdLo,
            final int eventIndex,
            final int symbolId,
            final long orderId,
            final long uid,
            final long rejectedSize,
            final long price) {
        final GatewayUserProfile profile = state.getUserProfile(uid);
        if (profile == null) {
            return;
        }
        final GatewayOrder order = profile.order(orderId);
        if (order == null) {
            return;
        }
        order.state(GatewayOrder.STATE_REJECTED);
        takerOrderByCommand.remove(order.commandIdLo());
    }

    private void trackTaker(final long commandIdLo, final GatewayOrder order) {
        if (takerOrderByCommand.size() >= MAX_TRACKED_TAKER_ORDERS) {
            takerOrderByCommand.clear();
        }
        takerOrderByCommand.put(commandIdLo, order);
    }

    // ------------------------------------------------------------------
    // In-flight list, envelope plumbing, and release
    // ------------------------------------------------------------------

    private int sweepExpired() {
        final long now = System.nanoTime();
        int swept = 0;
        GatewayRequest req = inFlightHead;
        while (req != null && now - req.deadlineNanos >= 0L) {
            final GatewayRequest next = req.nextInFlight;
            inFlight.remove(req.commandIdLo);
            unlinkInFlight(req);
            beginEnvelope(0, 0, "gateway timeout waiting for cluster result");
            json.valueNull().endObject();
            send(req, HttpResponseStatus.GATEWAY_TIMEOUT);
            release(req);
            swept++;
            req = next;
        }
        return swept;
    }

    private void linkInFlight(final GatewayRequest req) {
        final GatewayRequest tail = inFlightTail;
        req.prevInFlight = tail;
        req.nextInFlight = null;
        if (tail == null) {
            inFlightHead = req;
        } else {
            tail.nextInFlight = req;
        }
        inFlightTail = req;
    }

    private void unlinkInFlight(final GatewayRequest req) {
        final GatewayRequest prev = req.prevInFlight;
        final GatewayRequest next = req.nextInFlight;
        if (prev == null) {
            inFlightHead = next;
        } else {
            prev.nextInFlight = next;
        }
        if (next == null) {
            inFlightTail = prev;
        } else {
            next.prevInFlight = prev;
        }
        req.prevInFlight = null;
        req.nextInFlight = null;
    }

    private void beginEnvelope(final int gatewayResultCode, final int coreResultCode, final String description) {
        json.reset();
        json.beginObject()
                .name("ticket")
                .valueLong(0L)
                .name("gatewayResultCode")
                .valueLong(gatewayResultCode)
                .name("coreResultCode")
                .valueLong(coreResultCode)
                .name("description")
                .valueString(description)
                .name("data");
    }

    private void sendError(
            final GatewayRequest req,
            final HttpResponseStatus status,
            final int gatewayResultCode,
            final String description) {
        beginEnvelope(gatewayResultCode, 0, description);
        json.valueNull().endObject();
        send(req, status);
    }

    private void sendError(
            final GatewayRequest req, final HttpResponseStatus status, final ApiErrorCodes code, final String detail) {
        final String description = detail == null ? code.errorDescription : code.errorDescription + ": " + detail;
        sendError(req, status, code.gatewayErrorCode, description);
    }

    private void send(final GatewayRequest req, final HttpResponseStatus status) {
        HttpResponses.sendJson(req.ctx, status, json.buffer(), json.length(), req.keepAlive);
    }

    private void release(final GatewayRequest req) {
        req.reset();
        free.offer(req);
    }
}
