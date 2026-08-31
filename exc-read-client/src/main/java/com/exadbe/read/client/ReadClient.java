package com.exadbe.read.client;

import com.exadbe.protocol.MessageHeaderDecoder;
import com.exadbe.protocol.MessageHeaderEncoder;
import com.exadbe.protocol.OrderType;
import com.exadbe.protocol.QueryRequestEncoder;
import com.exadbe.protocol.QueryResponseDecoder;
import com.exadbe.protocol.QueryStatusCode;
import com.exadbe.protocol.QueryType;
import com.exadbe.read.client.config.ReadClientConfig;
import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The read-side SDK: queries a running read replica's {@code QueryResponder}
 * over plain Aeron request/response streams. Consumes only the
 * {@code exc-protocol} wire contract, never {@code exc-core} or
 * {@code exc-read}, mirroring how {@code ExcClient} stays decoupled from the
 * engine.
 *
 * <p>Two API modes share one core:
 *
 * <ul>
 *   <li><b>Asynchronous</b>: {@code submit...} returns a {@code requestId}
 *       without blocking (throwing {@link BackpressureException} when the
 *       in-flight window is full); {@link #poll()} drives delivery and fires
 *       the registered {@link QueryListener}; unanswered queries are
 *       re-published idempotently (same request id) until answered or the
 *       retry budget is exhausted, at which point {@code onTimeout} fires.
 *   <li><b>Synchronous</b>: the {@code balance(...)}-style methods submit and
 *       block (driving {@link #poll()} themselves) until the matching response
 *       arrives or {@code messageTimeoutNs} elapses, throwing
 *       {@link QueryTimeoutException}.
 * </ul>
 *
 * <p>Queries are reads, so retries simply re-publish the same request id; a
 * response to an abandoned attempt is discarded. Results are eventually
 * consistent and each carries the replica's {@code appliedPosition} at answer
 * time.
 *
 * <p>Not thread-safe: query methods and {@link #poll()} must be called from a
 * single thread; listener callbacks run on that same thread.
 */
public final class ReadClient implements AutoCloseable {

    private static final float LOAD_FACTOR = 0.65f;
    private static final int RESPONSE_FRAGMENT_LIMIT = 64;
    private static final int REQUEST_BUFFER_CAPACITY = 1024;
    private static final long RESOLVE_ENDPOINT_TIMEOUT_MS = 10_000L;

    private final ReadClientConfig config;
    private final MediaDriver ownMediaDriver;
    private final Aeron aeron;
    private final Subscription responses;
    private final Publication requests;
    private final FragmentAssembler fragmentAssembler;
    private final String responseChannel;
    private final IdleStrategy idle = new BackoffIdleStrategy();

    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final QueryRequestEncoder requestEncoder = new QueryRequestEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final QueryResponseDecoder responseDecoder = new QueryResponseDecoder();

    private final Long2ObjectHashMap<PendingQuery> pending;
    private final PendingQuery[] pool;
    private final int[] freeStack;
    private int freeTop;

    private QueryListener listener = QueryListener.NONE;
    private long nextRequestId = 1L;
    private long submitted;
    private long completed;
    private long expired;
    private long backpressureEvents;
    private long lastOfferResult;
    private long lastAppliedPosition;

    // Single-threaded stack of sync delivery frames. A stack (not a single slot)
    // lets a listener callback issue a nested synchronous query without
    // overwriting the outer await's frame.
    private static final int MAX_SYNC_NESTING = 8;
    private final SyncFrame[] syncStack = new SyncFrame[MAX_SYNC_NESTING];
    private int syncDepth;

    /**
     * @param config query endpoints and timing; a {@code null}
     *     aeronDirectoryName launches an embedded media driver
     */
    public ReadClient(final ReadClientConfig config) {
        this.config = config;
        this.pending = new Long2ObjectHashMap<>(Math.max(16, config.maxInFlight() * 2), LOAD_FACTOR);
        this.pool = new PendingQuery[config.maxInFlight()];
        this.freeStack = new int[config.maxInFlight()];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new PendingQuery(i);
            freeStack[i] = pool.length - 1 - i;
        }
        this.freeTop = pool.length;

        MediaDriver embedded = null;
        String dir = config.aeronDirectoryName();
        if (dir == null) {
            embedded = MediaDriver.launchEmbedded(new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true));
            dir = embedded.aeronDirectoryName();
        }
        this.ownMediaDriver = embedded;
        Aeron aeronClient = null;
        Subscription sub = null;
        Publication pub = null;
        String channel = null;
        try {
            aeronClient = Aeron.connect(new Aeron.Context().aeronDirectoryName(dir));
            sub = aeronClient.addSubscription(config.responseChannel(), config.responseStreamId());
            channel = awaitResolvedEndpoint(sub);
            pub = aeronClient.addPublication(config.requestChannel(), config.requestStreamId());
        } catch (final RuntimeException e) {
            if (pub != null) {
                pub.close();
            }
            if (sub != null) {
                sub.close();
            }
            if (aeronClient != null) {
                aeronClient.close();
            }
            if (embedded != null) {
                embedded.close();
            }
            throw e;
        }
        this.aeron = aeronClient;
        this.responses = sub;
        this.requests = pub;
        this.fragmentAssembler = new FragmentAssembler(this::onResponse);
        this.responseChannel = channel;
    }

    /** Registers the asynchronous delivery listener; replaces any previous listener. */
    public void setListener(final QueryListener listener) {
        this.listener = listener == null ? QueryListener.NONE : listener;
    }

    // ============================ Asynchronous API ============================

    /** Submits a {@code USER_EXISTS} query for {@code uid}; returns its request id. */
    public long submitUserExists(final long uid) {
        return submit(QueryType.USER_EXISTS, encoder -> encoder.uid(uid));
    }

    /** Submits a {@code BALANCE} query for {@code (uid, currency)}; returns its request id. */
    public long submitBalance(final long uid, final int currency) {
        return submit(QueryType.BALANCE, encoder -> encoder.uid(uid).currency(currency));
    }

    /** Submits an {@code L2_ORDER_BOOK} query; returns its request id. */
    public long submitOrderBook(final int symbolId, final int maxLevels) {
        return submit(
                QueryType.L2_ORDER_BOOK, encoder -> encoder.symbolId(symbolId).maxLevels(maxLevels));
    }

    /** Submits a {@code SINGLE_USER_REPORT} query; returns its request id. */
    public long submitSingleUserReport(final long uid) {
        return submit(QueryType.SINGLE_USER_REPORT, encoder -> encoder.uid(uid));
    }

    /** Submits an {@code ORDER_HISTORY} query; returns its request id. */
    public long submitOrderHistory(final long uid) {
        return submit(QueryType.ORDER_HISTORY, encoder -> encoder.uid(uid));
    }

    /** Submits an {@code ACTIVE_ORDERS} query; returns its request id. */
    public long submitActiveOrders(final long uid) {
        return submit(QueryType.ACTIVE_ORDERS, encoder -> encoder.uid(uid));
    }

    /** Submits an {@code ORDER_BY_ID} query; returns its request id. */
    public long submitOrderById(final long orderId) {
        return submit(QueryType.ORDER_BY_ID, encoder -> encoder.orderId(orderId));
    }

    /** Submits a {@code USER_TRADES} query; returns its request id. */
    public long submitUserTrades(final long uid, final int limit) {
        return submit(QueryType.USER_TRADES, encoder -> encoder.uid(uid).tradeLimit(limit));
    }

    /** Submits a {@code MARKET_TRADES} query; returns its request id. */
    public long submitMarketTrades(final int symbolId, final int limit) {
        return submit(
                QueryType.MARKET_TRADES, encoder -> encoder.symbolId(symbolId).tradeLimit(limit));
    }

    /** Submits a {@code TOTAL_CURRENCY_BALANCE} query; returns its request id. */
    public long submitTotalCurrencyBalance() {
        return submit(QueryType.TOTAL_CURRENCY_BALANCE, encoder -> {});
    }

    /** Submits a {@code STATE_HASH} query; returns its request id. */
    public long submitStateHash() {
        return submit(QueryType.STATE_HASH, encoder -> {});
    }

    /**
     * Drives response delivery, listener callbacks, and idempotent
     * retransmission of unanswered queries. Call in a loop.
     *
     * @return an opaque work count (positive when progress was made)
     */
    public int poll() {
        int work = responses.poll(fragmentAssembler, RESPONSE_FRAGMENT_LIMIT);
        work += retransmit(System.nanoTime());
        return work;
    }

    // ============================ Synchronous API ============================

    /** Whether the replicated state contains {@code uid}. */
    public boolean userExists(final long uid) {
        return (Boolean) await(submitUserExists(uid));
    }

    /** The balance of {@code (uid, currency)}; {@code found} is false when the account is unknown. */
    public BalanceResult balance(final long uid, final int currency) {
        return (BalanceResult) await(submitBalance(uid, currency));
    }

    /** An L2 snapshot of {@code symbolId}, at most {@code maxLevels} levels per side. */
    public L2Snapshot orderBook(final int symbolId, final int maxLevels) {
        return (L2Snapshot) await(submitOrderBook(symbolId, maxLevels));
    }

    /** Status, balances, and resting orders of {@code uid}. */
    public UserReport singleUserReport(final long uid) {
        return (UserReport) await(submitSingleUserReport(uid));
    }

    /** Every tracked order of {@code uid} in placement order. */
    public List<OrderRecordResult> orderHistory(final long uid) {
        return castRecords(await(submitOrderHistory(uid)));
    }

    /** The still-resting orders of {@code uid} in placement order. */
    public List<OrderRecordResult> activeOrders(final long uid) {
        return castRecords(await(submitActiveOrders(uid)));
    }

    /** The tracked record for {@code orderId}, or {@code null} when unknown. */
    public OrderRecordResult order(final long orderId) {
        return (OrderRecordResult) await(submitOrderById(orderId));
    }

    /** The most recent {@code limit} trades involving {@code uid} as maker or taker. */
    public List<MarketTradeResult> userTrades(final long uid, final int limit) {
        return castTrades(await(submitUserTrades(uid, limit)));
    }

    /** The most recent {@code limit} trades of {@code symbolId}. */
    public List<MarketTradeResult> marketTrades(final int symbolId, final int limit) {
        return castTrades(await(submitMarketTrades(symbolId, limit)));
    }

    /** Per-currency value conservation breakdown of the replicated state. */
    public TotalBalanceResult totalCurrencyBalance() {
        return (TotalBalanceResult) await(submitTotalCurrencyBalance());
    }

    /** The deterministic fingerprint of the replicated state. */
    public long stateHash() {
        return (Long) await(submitStateHash());
    }

    // ============================ Stats and diagnostics ============================

    /** The cluster log position the read service had applied when answering the most recent query. */
    public long lastAppliedPosition() {
        return lastAppliedPosition;
    }

    /** Number of queries submitted. */
    public long submitted() {
        return submitted;
    }

    /** Number of queries answered. */
    public long completed() {
        return completed;
    }

    /** Number of queries expired on their retry budget. */
    public long expired() {
        return expired;
    }

    /** Number of times a submit hit the in-flight window or an offer was backpressured. */
    public long backpressureEvents() {
        return backpressureEvents;
    }

    /** The raw result of the most recent request offer (diagnostics). */
    public long lastOfferResult() {
        return lastOfferResult;
    }

    // ============================ Core ============================

    @SuppressWarnings("unchecked")
    private static List<OrderRecordResult> castRecords(final Object value) {
        return (List<OrderRecordResult>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<MarketTradeResult> castTrades(final Object value) {
        return (List<MarketTradeResult>) value;
    }

    @FunctionalInterface
    private interface RequestFiller {
        void fill(QueryRequestEncoder encoder);
    }

    private long submit(final QueryType type, final RequestFiller filler) {
        if (freeTop == 0) {
            backpressureEvents++;
            throw new BackpressureException("query in-flight window full: " + config.maxInFlight());
        }
        final PendingQuery pq = pool[freeStack[--freeTop]];
        pq.inUse = true;
        pq.retries = 0;
        pq.requestId = nextRequestId();
        pq.type = type;
        pq.submittedNanos = System.nanoTime();

        requestEncoder
                .wrapAndApplyHeader(pq.buffer, 0, headerEncoder)
                .requestId(pq.requestId)
                .queryType(type)
                .uid(QueryRequestEncoder.uidNullValue())
                .currency(QueryRequestEncoder.currencyNullValue())
                .symbolId(QueryRequestEncoder.symbolIdNullValue())
                .orderId(QueryRequestEncoder.orderIdNullValue())
                .tradeLimit(QueryRequestEncoder.tradeLimitNullValue())
                .maxLevels(QueryRequestEncoder.maxLevelsNullValue())
                .responseStreamId(config.responseStreamId())
                .responseChannel(responseChannel);
        filler.fill(requestEncoder);
        pq.length = MessageHeaderEncoder.ENCODED_LENGTH + requestEncoder.encodedLength();
        pq.deadlineNanos = pq.submittedNanos + config.retryBackoffNs();

        pending.put(pq.requestId, pq);
        submitted++;
        offer(pq);
        return pq.requestId;
    }

    // Request id 0 is reserved (it never collides with a live id after a wrap,
    // since the id is always positive); wrap back to 1 rather than through 0.
    private long nextRequestId() {
        final long id = nextRequestId;
        nextRequestId = (nextRequestId == Long.MAX_VALUE) ? 1L : nextRequestId + 1L;
        return id;
    }

    private void offer(final PendingQuery pq) {
        lastOfferResult = requests.offer(pq.buffer, 0, pq.length);
        if (lastOfferResult < 0) {
            backpressureEvents++;
        }
    }

    private int retransmit(final long now) {
        if (freeTop == pool.length) {
            // Nothing in flight: the per-cycle pool scan would be pure waste.
            return 0;
        }
        int work = 0;
        for (int i = 0; i < pool.length; i++) {
            final PendingQuery pq = pool[i];
            // A delivering slot is mid-listener-callback (which may re-enter
            // poll via a nested synchronous query); it is released when the
            // callback returns, so it must not be expired here as well.
            if (!pq.inUse || pq.delivering || (now - pq.deadlineNanos) < 0) {
                continue;
            }
            // An overall budget bounds every async query, even when maxRetries is 0
            // (unbounded retries); without it a dead replica would retransmit
            // forever and exhaust the in-flight window.
            if (now - pq.submittedNanos > config.messageTimeoutNs()) {
                expire(pq);
                continue;
            }
            if (config.maxRetries() > 0 && pq.retries >= config.maxRetries()) {
                expire(pq);
                continue;
            }
            offer(pq);
            pq.retries++;
            pq.deadlineNanos = now + config.retryBackoffNs();
            work++;
        }
        return work;
    }

    private void expire(final PendingQuery pq) {
        pending.remove(pq.requestId);
        expired++;
        if (listener != QueryListener.NONE) {
            listener.onTimeout(pq.requestId, pq.type);
        }
        release(pq);
    }

    private void onResponse(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        headerDecoder.wrap(buffer, offset);
        if (headerDecoder.schemaId() != MessageHeaderDecoder.SCHEMA_ID
                || headerDecoder.templateId() != QueryResponseDecoder.TEMPLATE_ID
                || length < MessageHeaderDecoder.ENCODED_LENGTH + headerDecoder.blockLength()) {
            return;
        }
        responseDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(),
                headerDecoder.version());
        final long requestId = responseDecoder.requestId();
        final PendingQuery pq = pending.get(requestId);
        if (pq == null) {
            return; // Late response to an abandoned or already-completed query.
        }
        decodeResponse(pq);
        lastAppliedPosition = pq.appliedPosition;
        pending.remove(requestId);
        // The listener callback may re-enter poll() (a nested synchronous
        // query); the slot is off the pending map but not yet released, so mark
        // it delivering to keep a reentrant retransmit from expiring it and
        // releasing the pool slot twice.
        pq.delivering = true;
        final SyncFrame frame = syncFrameFor(requestId);
        if (pq.status == QueryStatusCode.UNSUPPORTED) {
            if (listener != QueryListener.NONE) {
                listener.onError(requestId, pq.type, pq.status);
            }
            if (frame != null) {
                frame.error = new QueryException(pq.status, "read service rejected query type: " + pq.type);
            }
        } else {
            deliver(pq);
            if (frame != null) {
                frame.value = pq.value;
                frame.delivered = true;
            }
        }
        release(pq);
        completed++;
    }

    private void deliver(final PendingQuery pq) {
        if (listener == QueryListener.NONE) {
            return;
        }
        switch (pq.type) {
            case USER_EXISTS -> listener.onUserExists(pq.requestId, (Boolean) pq.value);
            case BALANCE -> listener.onBalance(pq.requestId, (BalanceResult) pq.value);
            case L2_ORDER_BOOK -> listener.onL2(pq.requestId, (L2Snapshot) pq.value);
            case SINGLE_USER_REPORT -> listener.onUserReport(pq.requestId, (UserReport) pq.value);
            case ORDER_HISTORY -> listener.onOrderHistory(pq.requestId, castRecords(pq.value));
            case ACTIVE_ORDERS -> listener.onActiveOrders(pq.requestId, castRecords(pq.value));
            case ORDER_BY_ID -> listener.onOrder(pq.requestId, (OrderRecordResult) pq.value);
            case USER_TRADES -> listener.onUserTrades(pq.requestId, castTrades(pq.value));
            case MARKET_TRADES -> listener.onMarketTrades(pq.requestId, castTrades(pq.value));
            case TOTAL_CURRENCY_BALANCE -> listener.onTotalCurrencyBalance(pq.requestId, (TotalBalanceResult) pq.value);
            case STATE_HASH -> listener.onStateHash(pq.requestId, (Long) pq.value);
            default -> listener.onError(pq.requestId, pq.type, pq.status);
        }
    }

    /**
     * Blocks (driving {@link #poll()}) until the submitted query is delivered
     * or {@code messageTimeoutNs} elapses. The awaited request id is pushed on a
     * stack, so a listener callback that issues its own synchronous query during
     * {@link #poll()} does not corrupt this (outer) await.
     */
    @SuppressWarnings("unchecked")
    private <T> T await(final long requestId) {
        final SyncFrame frame = pushSyncFrame(requestId);
        final long deadline = System.nanoTime() + config.messageTimeoutNs();
        try {
            while (true) {
                poll();
                if (frame.error != null) {
                    final QueryException error = frame.error;
                    frame.error = null;
                    throw error;
                }
                if (frame.delivered) {
                    final T value = (T) frame.value;
                    frame.value = null;
                    frame.delivered = false;
                    return value;
                }
                if (System.nanoTime() >= deadline) {
                    // Release the abandoned query's window slot: a dead replica
                    // must not keep the slot occupied by a never-answered query.
                    cancel(requestId);
                    throw new QueryTimeoutException("no response for query requestId=" + requestId);
                }
                idle.idle(0);
            }
        } finally {
            popSyncFrame();
        }
    }

    private SyncFrame pushSyncFrame(final long requestId) {
        if (syncDepth == MAX_SYNC_NESTING) {
            throw new IllegalStateException("synchronous query nesting exceeds " + MAX_SYNC_NESTING);
        }
        SyncFrame frame = syncStack[syncDepth];
        if (frame == null) {
            frame = new SyncFrame();
            syncStack[syncDepth] = frame;
        }
        frame.requestId = requestId;
        frame.delivered = false;
        frame.value = null;
        frame.error = null;
        syncDepth++;
        return frame;
    }

    private void popSyncFrame() {
        syncDepth--;
    }

    private SyncFrame syncFrameFor(final long requestId) {
        if (syncDepth > 0 && syncStack[syncDepth - 1].requestId == requestId) {
            return syncStack[syncDepth - 1];
        }
        return null;
    }

    private void cancel(final long requestId) {
        final PendingQuery pq = pending.remove(requestId);
        if (pq != null) {
            release(pq);
        }
    }

    private void decodeResponse(final PendingQuery pq) {
        pq.status = responseDecoder.status();
        pq.appliedPosition = responseDecoder.appliedPosition();
        switch (responseDecoder.queryType()) {
            case USER_EXISTS -> pq.value = responseDecoder.userExists() != 0;
            case BALANCE -> pq.value =
                    new BalanceResult(responseDecoder.balance(), responseDecoder.userExists() != 0, pq.appliedPosition);
            case L2_ORDER_BOOK -> pq.value = decodeL2(pq);
            case SINGLE_USER_REPORT -> pq.value = decodeUserReport(pq);
            case ORDER_HISTORY -> pq.value = decodeHistory();
            case ACTIVE_ORDERS -> pq.value = decodeHistory();
            case ORDER_BY_ID -> {
                final List<OrderRecordResult> records = decodeHistory();
                pq.value = records.isEmpty() ? null : records.get(0);
            }
            case USER_TRADES -> pq.value = decodeTrades();
            case MARKET_TRADES -> pq.value = decodeTrades();
            case TOTAL_CURRENCY_BALANCE -> pq.value = decodeTotals(pq);
            case STATE_HASH -> pq.value = responseDecoder.stateHash();
            default -> pq.status = QueryStatusCode.UNSUPPORTED;
        }
    }

    private L2Snapshot decodeL2(final PendingQuery pq) {
        final int symbolId = responseDecoder.symbolId();
        final ArrayList<L2Snapshot.Level> asks = new ArrayList<>();
        final QueryResponseDecoder.AsksDecoder asksDecoder = responseDecoder.asks();
        while (asksDecoder.hasNext()) {
            final QueryResponseDecoder.AsksDecoder element = asksDecoder.next();
            asks.add(new L2Snapshot.Level(element.price(), element.size(), element.orders()));
        }
        final ArrayList<L2Snapshot.Level> bids = new ArrayList<>();
        final QueryResponseDecoder.BidsDecoder bidsDecoder = responseDecoder.bids();
        while (bidsDecoder.hasNext()) {
            final QueryResponseDecoder.BidsDecoder element = bidsDecoder.next();
            bids.add(new L2Snapshot.Level(element.price(), element.size(), element.orders()));
        }
        return new L2Snapshot(symbolId, pq.status == QueryStatusCode.SUCCESS, pq.appliedPosition, asks, bids);
    }

    private UserReport decodeUserReport(final PendingQuery pq) {
        final long uid = responseDecoder.uid();
        final ArrayList<UserReport.Balance> balances = new ArrayList<>();
        final QueryResponseDecoder.BalancesDecoder balancesDecoder = responseDecoder.balances();
        while (balancesDecoder.hasNext()) {
            final QueryResponseDecoder.BalancesDecoder element = balancesDecoder.next();
            balances.add(new UserReport.Balance(element.currency(), element.balance()));
        }
        final ArrayList<UserReport.RestingOrder> orders = new ArrayList<>();
        final QueryResponseDecoder.OrdersDecoder ordersDecoder = responseDecoder.orders();
        while (ordersDecoder.hasNext()) {
            final QueryResponseDecoder.OrdersDecoder element = ordersDecoder.next();
            orders.add(new UserReport.RestingOrder(
                    element.symbolId(),
                    element.orderId(),
                    element.ask() != 0,
                    element.price(),
                    element.size(),
                    element.filled(),
                    element.reserveBidPrice()));
        }
        return new UserReport(
                uid,
                responseDecoder.userExists() != 0,
                responseDecoder.suspended() != 0,
                pq.appliedPosition,
                balances,
                orders);
    }

    private List<OrderRecordResult> decodeHistory() {
        final ArrayList<OrderRecordResult> records = new ArrayList<>();
        final QueryResponseDecoder.HistoryDecoder history = responseDecoder.history();
        while (history.hasNext()) {
            final QueryResponseDecoder.HistoryDecoder element = history.next();
            final ArrayList<OrderRecordResult.FillResult> fills = new ArrayList<>();
            final QueryResponseDecoder.HistoryDecoder.FillsDecoder fillsDecoder = element.fills();
            while (fillsDecoder.hasNext()) {
                final QueryResponseDecoder.HistoryDecoder.FillsDecoder fill = fillsDecoder.next();
                fills.add(new OrderRecordResult.FillResult(
                        fill.taker() != 0, fill.price(), fill.size(), fill.counterpartyUid(), fill.timestamp()));
            }
            records.add(new OrderRecordResult(
                    element.symbolId(),
                    element.orderId(),
                    element.uid(),
                    element.ask() != 0,
                    orderTypeName(element.orderType()),
                    element.price(),
                    element.size(),
                    element.filled(),
                    element.reduced(),
                    element.placedTimestamp(),
                    element.lastTimestamp(),
                    element.userCookie(),
                    element.state(),
                    OrderState.name(element.state()),
                    fills));
        }
        return records;
    }

    private List<MarketTradeResult> decodeTrades() {
        final ArrayList<MarketTradeResult> trades = new ArrayList<>();
        final QueryResponseDecoder.TradesDecoder tradesDecoder = responseDecoder.trades();
        while (tradesDecoder.hasNext()) {
            final QueryResponseDecoder.TradesDecoder element = tradesDecoder.next();
            trades.add(new MarketTradeResult(
                    element.timestamp(),
                    element.symbolId(),
                    element.price(),
                    element.size(),
                    element.makerOrderId(),
                    element.makerUid(),
                    element.takerUid()));
        }
        return trades;
    }

    private TotalBalanceResult decodeTotals(final PendingQuery pq) {
        final ArrayList<TotalBalanceResult.Total> totals = new ArrayList<>();
        final QueryResponseDecoder.TotalsDecoder totalsDecoder = responseDecoder.totals();
        while (totalsDecoder.hasNext()) {
            final QueryResponseDecoder.TotalsDecoder element = totalsDecoder.next();
            totals.add(new TotalBalanceResult.Total(
                    element.currency(), element.accountBalance(), element.reserved(), element.fees()));
        }
        return new TotalBalanceResult(pq.appliedPosition, totals);
    }

    private static String orderTypeName(final OrderType type) {
        switch (type) {
            case GTC:
                return "GTC";
            case IOC:
                return "IOC";
            case FOK_BUDGET:
                return "FOK_BUDGET";
            default:
                return "UNKNOWN";
        }
    }

    private static String awaitResolvedEndpoint(final Subscription subscription) {
        final long deadline = System.currentTimeMillis() + RESOLVE_ENDPOINT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            final String endpoint = subscription.resolvedEndpoint();
            if (endpoint != null) {
                return "aeron:udp?endpoint=" + endpoint;
            }
            Thread.onSpinWait();
        }
        throw new IllegalStateException("timed out resolving the query response endpoint");
    }

    private void release(final PendingQuery pq) {
        pq.inUse = false;
        pq.delivering = false;
        pq.value = null;
        pq.status = QueryStatusCode.SUCCESS;
        pq.appliedPosition = 0L;
        pq.submittedNanos = 0L;
        freeStack[freeTop++] = pq.poolIndex;
    }

    @Override
    public void close() {
        requests.close();
        responses.close();
        aeron.close();
        if (ownMediaDriver != null) {
            ownMediaDriver.close();
        }
    }

    /** One frame of an in-progress synchronous await (see {@link #await}). */
    private static final class SyncFrame {
        long requestId;
        boolean delivered;
        Object value;
        QueryException error;
    }

    /** One in-flight query; pooled with a private request buffer so retransmits re-offer the same bytes. */
    private static final class PendingQuery {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[REQUEST_BUFFER_CAPACITY]);
        final int poolIndex;
        long requestId;
        QueryType type;
        int length;
        int retries;
        long deadlineNanos;
        long submittedNanos;
        boolean inUse;
        boolean delivering;
        Object value;
        QueryStatusCode status;
        long appliedPosition;

        PendingQuery(final int poolIndex) {
            this.poolIndex = poolIndex;
        }
    }
}
