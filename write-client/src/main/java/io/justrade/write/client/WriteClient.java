package io.justrade.write.client;

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.justrade.protocol.CommandEnvelopeEncoder;
import io.justrade.protocol.CommandResultDecoder;
import io.justrade.protocol.L2MarketDataDecoder;
import io.justrade.protocol.MessageHeaderDecoder;
import io.justrade.protocol.MessageHeaderEncoder;
import io.justrade.protocol.OrderAction;
import io.justrade.protocol.OrderCommandType;
import io.justrade.protocol.OrderType;
import io.justrade.protocol.ReduceEventDecoder;
import io.justrade.protocol.RejectEventDecoder;
import io.justrade.protocol.TradeEventDecoder;
import io.justrade.write.client.config.ClientConfig;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * Edge-side client for the justrade core. Adds, on top of a raw Aeron cluster
 * client:
 *
 * <ul>
 *   <li>leader-change handling (resends in-flight commands to the new leader);
 *   <li>idempotent retry that reuses the original command id, which is the
 *       precondition for the core's dedup guarantee;
 *   <li>asynchronous request/response correlation by command id;
 *   <li>explicit backpressure signalling to the caller (never a silent drop);
 *   <li>end-to-end latency measurement via HdrHistogram.
 * </ul>
 *
 * <p>Not thread-safe: {@link #submit} and {@link #poll} must be called from the
 * same thread. Steady-state submission is allocation-free (pending commands are
 * pooled).
 */
public final class WriteClient implements EgressListener, AutoCloseable {

    private static final float LOAD_FACTOR = 0.65f;

    private final ClientConfig config;
    private final ResultHandler handler;
    private AeronCluster cluster;
    private final String aeronDir;
    private final MediaDriver ownMediaDriver;

    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final CommandEnvelopeEncoder envelopeEncoder = new CommandEnvelopeEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final CommandResultDecoder resultDecoder = new CommandResultDecoder();
    private final TradeEventDecoder tradeDecoder = new TradeEventDecoder();
    private final ReduceEventDecoder reduceDecoder = new ReduceEventDecoder();
    private final RejectEventDecoder rejectDecoder = new RejectEventDecoder();
    private final L2MarketDataDecoder l2Decoder = new L2MarketDataDecoder();

    private TradeEventListener tradeListener = TradeEventListener.NONE;
    private ReduceEventListener reduceListener = ReduceEventListener.NONE;
    private RejectEventListener rejectListener = RejectEventListener.NONE;
    private OrderBookListener orderBookListener = OrderBookListener.NONE;
    private TradeGroupListener tradeGroupListener = TradeGroupListener.NONE;

    // Reusable event holders, filled in place on the poll thread.
    private final OrderBookSnapshot snapshot = new OrderBookSnapshot();
    private final TradeGroup tradeGroup = new TradeGroup();

    // The event stream announced by the most recent CommandResult: -1 when the
    // count is unknown (pre-v3 peer), otherwise frames still expected.
    private long eventStreamIdLo;
    private int eventStreamRemaining = -1;

    private final Long2ObjectHashMap<PendingCommand> pending;
    private final PendingCommand[] pool;
    private final int[] freeStack;
    private int freeTop;

    // Reusable scratch for retransmitDue: the pool indices of every due
    // command, sorted by clientSeq before re-offer (submission order).
    private final int[] dueOrder;

    private final Histogram latencyHistogram = new Histogram(3_600_000_000_000L, 3);

    private long nextClientSeq;
    private long nextCommandIdLo = 1L;

    // The oldest submitted command the driver has not accepted yet. Submission
    // order must equal cluster delivery order, so while it is queued every later
    // submit is backpressured and poll() re-offers it before anything else.
    private PendingCommand queuedUnsent;

    private long submitted;
    private long completed;
    private long expired;
    private long backpressureEvents;
    private long firstNegativeOfferResult;
    private long keepalives;
    private long retransmits;
    private long firstRetransmitIdLo = Long.MIN_VALUE;
    private long firstExpiredIdLo = Long.MIN_VALUE;
    private int reconnects;
    private int reconnectFailures;
    private int leaderChanges;
    private int leaderMemberId = -1;
    private boolean retransmitAll;

    // Session liveness: the cluster closes idle sessions after its session
    // timeout (10 s default), so an idle client submits a NOP keepalive. When
    // the session is lost anyway (cluster restart, egress CLOSED / ERROR), the
    // client reconnects on the next polls and retransmits everything pending.
    private long nextKeepaliveNanos;
    private boolean sessionLost;
    private long nextReconnectNanos;

    public WriteClient(final ClientConfig config, final ResultHandler handler) {
        this.config = config;
        this.handler = handler;
        // Size the pending map so it never rehashes while the in-flight window
        // (bounded by maxInFlight) is populated.
        this.pending = new Long2ObjectHashMap<>(Math.max(16, config.maxInFlight() * 2), LOAD_FACTOR);
        this.pool = new PendingCommand[config.maxInFlight()];
        this.freeStack = new int[config.maxInFlight()];
        this.dueOrder = new int[config.maxInFlight()];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new PendingCommand(i);
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
        this.aeronDir = dir;

        try {
            this.cluster = AeronCluster.connect(clusterContext(config.messageTimeoutNs()));
        } catch (final RuntimeException e) {
            if (embedded != null) {
                embedded.close();
            }
            throw e;
        }
        this.nextClientSeq = config.initialClientSeq();
        this.nextKeepaliveNanos = System.nanoTime() + config.keepaliveIntervalNs();
    }

    // Reconnect reuses the same driver and endpoints as the initial connect.
    private AeronCluster.Context clusterContext(final long messageTimeoutNs) {
        return new AeronCluster.Context()
                .egressListener(this)
                .aeronDirectoryName(aeronDir)
                .ingressChannel("aeron:udp")
                .egressChannel(config.egressChannel())
                .messageTimeoutNs(messageTimeoutNs)
                .ingressEndpoints(config.ingressEndpoints());
    }

    /**
     * Encodes and submits a command carrying only the mandatory identity and
     * {@code uid} fields (all order / account fields null). Returns its low
     * command-id word for correlation. The command is retried automatically
     * (reusing the same id) until acknowledged, on timeout or leader change.
     *
     * @throws BackpressureException if the in-flight window is full; the caller
     *     must poll and retry rather than have the command silently dropped.
     */
    public long submit(final OrderCommandType type, final long uid) {
        return encodeAndSubmit(
                type, uid, CommandEnvelopeEncoder.currencyNullValue(), CommandEnvelopeEncoder.balanceAmountNullValue());
    }

    /** Submits an {@code ADD_USER} command for {@code uid}. */
    public long addUser(final long uid) {
        return submit(OrderCommandType.ADD_USER, uid);
    }

    /** Suspends {@code uid}, blocking new order placement until resumed. */
    public long suspendUser(final long uid) {
        return submit(OrderCommandType.SUSPEND_USER, uid);
    }

    /** Resumes a previously suspended {@code uid}. */
    public long resumeUser(final long uid) {
        return submit(OrderCommandType.RESUME_USER, uid);
    }

    /** Submits a signed {@code BALANCE_ADJUSTMENT} for {@code (uid, currency)}. */
    public long adjustBalance(final long uid, final int currency, final long amount) {
        return encodeAndSubmit(OrderCommandType.BALANCE_ADJUSTMENT, uid, currency, amount);
    }

    /** Registers a spot symbol specification with zero fees. */
    public long addSymbol(
            final int symbolId,
            final int baseCurrency,
            final int quoteCurrency,
            final long baseScaleK,
            final long quoteScaleK) {
        return addSymbol(symbolId, baseCurrency, quoteCurrency, baseScaleK, quoteScaleK, 0L, 0L);
    }

    /** Registers a spot symbol specification with per-lot taker/maker fees. */
    public long addSymbol(
            final int symbolId,
            final int baseCurrency,
            final int quoteCurrency,
            final long baseScaleK,
            final long quoteScaleK,
            final long takerFee,
            final long makerFee) {
        if (windowFullForSubmit()) {
            backpressureEvents++;
            throw new BackpressureException("in-flight window full: " + config.maxInFlight());
        }

        final PendingCommand pc = pool[freeStack[--freeTop]];
        pc.inUse = true;
        pc.retries = 0;
        pc.commandIdHi = config.clientId();
        pc.commandIdLo = nextCommandIdLo++;
        final long clientSeq = nextClientSeq++;
        pc.clientSeq = clientSeq;

        envelopeEncoder
                .wrapAndApplyHeader(pc.buffer, 0, headerEncoder)
                .clientId(config.clientId())
                .clientSeq(clientSeq)
                .commandIdHi(pc.commandIdHi)
                .commandIdLo(pc.commandIdLo)
                .commandType(OrderCommandType.ADD_SYMBOL)
                .uid(0L)
                .symbolId(symbolId)
                .orderId(CommandEnvelopeEncoder.orderIdNullValue())
                .price(CommandEnvelopeEncoder.priceNullValue())
                .reserveBidPrice(CommandEnvelopeEncoder.reserveBidPriceNullValue())
                .size(CommandEnvelopeEncoder.sizeNullValue())
                .action(OrderAction.NULL_VAL)
                .orderType(OrderType.NULL_VAL)
                .userCookie(CommandEnvelopeEncoder.userCookieNullValue())
                .currency(CommandEnvelopeEncoder.currencyNullValue())
                .balanceAmount(CommandEnvelopeEncoder.balanceAmountNullValue())
                .baseCurrency(baseCurrency)
                .quoteCurrency(quoteCurrency)
                .baseScaleK(baseScaleK)
                .quoteScaleK(quoteScaleK)
                .takerFee(takerFee)
                .makerFee(makerFee);

        pc.length = MessageHeaderEncoder.ENCODED_LENGTH + envelopeEncoder.encodedLength();
        pc.submitNanos = System.nanoTime();
        pc.deadlineNanos = pc.submitNanos + config.retryBackoffNs();
        pending.put(pc.commandIdLo, pc);
        submitted++;
        offerUntilSent(pc);
        return pc.commandIdLo;
    }

    /** Registers a listener for trade events; replaces any previous listener. */
    public void tradeListener(final TradeEventListener listener) {
        this.tradeListener = listener == null ? TradeEventListener.NONE : listener;
    }

    /** Registers a listener for reduce events; replaces any previous listener. */
    public void reduceListener(final ReduceEventListener listener) {
        this.reduceListener = listener == null ? ReduceEventListener.NONE : listener;
    }

    /** Registers a listener for reject events; replaces any previous listener. */
    public void rejectListener(final RejectEventListener listener) {
        this.rejectListener = listener == null ? RejectEventListener.NONE : listener;
    }

    /** Registers a listener for L2 order-book snapshots; replaces any previous listener. */
    public void orderBookListener(final OrderBookListener listener) {
        this.orderBookListener = listener == null ? OrderBookListener.NONE : listener;
    }

    /** Registers a listener for per-command trade groups; replaces any previous listener. */
    public void tradeGroupListener(final TradeGroupListener listener) {
        this.tradeGroupListener = listener == null ? TradeGroupListener.NONE : listener;
    }

    /** Submits a GTC limit order. */
    public long placeGtc(
            final int symbolId,
            final long orderId,
            final boolean ask,
            final long price,
            final long size,
            final long reserveBidPrice,
            final long uid,
            final int userCookie) {
        return submitOrder(
                OrderCommandType.PLACE_ORDER,
                symbolId,
                orderId,
                uid,
                ask ? OrderAction.ASK : OrderAction.BID,
                OrderType.GTC,
                price,
                reserveBidPrice,
                size,
                userCookie);
    }

    /** Submits an IOC limit order. A bid reserves quote at the limit price. */
    public long placeIoc(
            final int symbolId,
            final long orderId,
            final boolean ask,
            final long price,
            final long size,
            final long uid,
            final int userCookie) {
        return submitOrder(
                OrderCommandType.PLACE_ORDER,
                symbolId,
                orderId,
                uid,
                ask ? OrderAction.ASK : OrderAction.BID,
                OrderType.IOC,
                price,
                ask ? CommandEnvelopeEncoder.reserveBidPriceNullValue() : price,
                size,
                userCookie);
    }

    /** Submits a fill-or-kill budget order ({@code budget} is the total price limit). */
    public long placeFokBudget(
            final int symbolId,
            final long orderId,
            final boolean ask,
            final long budget,
            final long size,
            final long uid,
            final int userCookie) {
        return submitOrder(
                OrderCommandType.PLACE_ORDER,
                symbolId,
                orderId,
                uid,
                ask ? OrderAction.ASK : OrderAction.BID,
                OrderType.FOK_BUDGET,
                budget,
                CommandEnvelopeEncoder.reserveBidPriceNullValue(),
                size,
                userCookie);
    }

    /** Cancels a resting order. */
    public long cancelOrder(final int symbolId, final long orderId, final long uid) {
        return submitOrder(
                OrderCommandType.CANCEL_ORDER,
                symbolId,
                orderId,
                uid,
                OrderAction.NULL_VAL,
                OrderType.NULL_VAL,
                CommandEnvelopeEncoder.priceNullValue(),
                CommandEnvelopeEncoder.reserveBidPriceNullValue(),
                CommandEnvelopeEncoder.sizeNullValue(),
                CommandEnvelopeEncoder.userCookieNullValue());
    }

    /** Moves a resting order to {@code newPrice}. */
    public long moveOrder(final int symbolId, final long orderId, final long newPrice, final long uid) {
        return submitOrder(
                OrderCommandType.MOVE_ORDER,
                symbolId,
                orderId,
                uid,
                OrderAction.NULL_VAL,
                OrderType.NULL_VAL,
                newPrice,
                CommandEnvelopeEncoder.reserveBidPriceNullValue(),
                CommandEnvelopeEncoder.sizeNullValue(),
                CommandEnvelopeEncoder.userCookieNullValue());
    }

    /** Reduces a resting order by {@code size}. */
    public long reduceOrder(final int symbolId, final long orderId, final long size, final long uid) {
        return submitOrder(
                OrderCommandType.REDUCE_ORDER,
                symbolId,
                orderId,
                uid,
                OrderAction.NULL_VAL,
                OrderType.NULL_VAL,
                CommandEnvelopeEncoder.priceNullValue(),
                CommandEnvelopeEncoder.reserveBidPriceNullValue(),
                size,
                CommandEnvelopeEncoder.userCookieNullValue());
    }

    /** Requests an L2 order-book snapshot for {@code symbolId}. */
    public long requestOrderBook(final int symbolId, final long uid) {
        return submitOrder(
                OrderCommandType.ORDER_BOOK_REQUEST,
                symbolId,
                CommandEnvelopeEncoder.orderIdNullValue(),
                uid,
                OrderAction.NULL_VAL,
                OrderType.NULL_VAL,
                CommandEnvelopeEncoder.priceNullValue(),
                CommandEnvelopeEncoder.reserveBidPriceNullValue(),
                CommandEnvelopeEncoder.sizeNullValue(),
                CommandEnvelopeEncoder.userCookieNullValue());
    }

    private long encodeAndSubmit(
            final OrderCommandType type, final long uid, final int currency, final long balanceAmount) {
        if (windowFullForSubmit()) {
            backpressureEvents++;
            throw new BackpressureException("in-flight window full: " + config.maxInFlight());
        }
        return encodeAndSubmitInner(type, uid, currency, balanceAmount);
    }

    // A fully-saturated window must not starve session liveness: when keepalives
    // are enabled, one pool slot is reserved for the NOP keepalive (normal
    // submits stop one slot early; submitKeepalive uses the reserved slot).
    private boolean windowFullForSubmit() {
        if (queuedUnsent != null) {
            return true;
        }
        return config.keepaliveIntervalNs() > 0 ? freeTop <= 1 : freeTop == 0;
    }

    private long encodeAndSubmitInner(
            final OrderCommandType type, final long uid, final int currency, final long balanceAmount) {
        final PendingCommand pc = pool[freeStack[--freeTop]];
        pc.inUse = true;
        pc.retries = 0;
        pc.commandIdHi = config.clientId();
        pc.commandIdLo = nextCommandIdLo++;
        final long clientSeq = nextClientSeq++;
        pc.clientSeq = clientSeq;

        envelopeEncoder
                .wrapAndApplyHeader(pc.buffer, 0, headerEncoder)
                .clientId(config.clientId())
                .clientSeq(clientSeq)
                .commandIdHi(pc.commandIdHi)
                .commandIdLo(pc.commandIdLo)
                .commandType(type)
                .uid(uid)
                .symbolId(CommandEnvelopeEncoder.symbolIdNullValue())
                .orderId(CommandEnvelopeEncoder.orderIdNullValue())
                .price(CommandEnvelopeEncoder.priceNullValue())
                .reserveBidPrice(CommandEnvelopeEncoder.reserveBidPriceNullValue())
                .size(CommandEnvelopeEncoder.sizeNullValue())
                .action(OrderAction.NULL_VAL)
                .orderType(OrderType.NULL_VAL)
                .userCookie(CommandEnvelopeEncoder.userCookieNullValue())
                .currency(currency)
                .balanceAmount(balanceAmount)
                .baseCurrency(CommandEnvelopeEncoder.baseCurrencyNullValue())
                .quoteCurrency(CommandEnvelopeEncoder.quoteCurrencyNullValue())
                .baseScaleK(CommandEnvelopeEncoder.baseScaleKNullValue())
                .quoteScaleK(CommandEnvelopeEncoder.quoteScaleKNullValue())
                .takerFee(CommandEnvelopeEncoder.takerFeeNullValue())
                .makerFee(CommandEnvelopeEncoder.makerFeeNullValue());

        pc.length = MessageHeaderEncoder.ENCODED_LENGTH + envelopeEncoder.encodedLength();
        pc.submitNanos = System.nanoTime();
        pc.deadlineNanos = pc.submitNanos + config.retryBackoffNs();

        pending.put(pc.commandIdLo, pc);
        submitted++;
        offerUntilSent(pc);
        return pc.commandIdLo;
    }

    private long submitOrder(
            final OrderCommandType type,
            final int symbolId,
            final long orderId,
            final long uid,
            final OrderAction action,
            final OrderType orderType,
            final long price,
            final long reserveBidPrice,
            final long size,
            final int userCookie) {
        if (windowFullForSubmit()) {
            backpressureEvents++;
            throw new BackpressureException("in-flight window full: " + config.maxInFlight());
        }

        final PendingCommand pc = pool[freeStack[--freeTop]];
        pc.inUse = true;
        pc.retries = 0;
        pc.commandIdHi = config.clientId();
        pc.commandIdLo = nextCommandIdLo++;
        final long clientSeq = nextClientSeq++;
        pc.clientSeq = clientSeq;

        envelopeEncoder
                .wrapAndApplyHeader(pc.buffer, 0, headerEncoder)
                .clientId(config.clientId())
                .clientSeq(clientSeq)
                .commandIdHi(pc.commandIdHi)
                .commandIdLo(pc.commandIdLo)
                .commandType(type)
                .uid(uid)
                .symbolId(symbolId)
                .orderId(orderId)
                .price(price)
                .reserveBidPrice(reserveBidPrice)
                .size(size)
                .action(action)
                .orderType(orderType)
                .userCookie(userCookie)
                .currency(CommandEnvelopeEncoder.currencyNullValue())
                .balanceAmount(CommandEnvelopeEncoder.balanceAmountNullValue())
                .baseCurrency(CommandEnvelopeEncoder.baseCurrencyNullValue())
                .quoteCurrency(CommandEnvelopeEncoder.quoteCurrencyNullValue())
                .baseScaleK(CommandEnvelopeEncoder.baseScaleKNullValue())
                .quoteScaleK(CommandEnvelopeEncoder.quoteScaleKNullValue())
                .takerFee(CommandEnvelopeEncoder.takerFeeNullValue())
                .makerFee(CommandEnvelopeEncoder.makerFeeNullValue());

        pc.length = MessageHeaderEncoder.ENCODED_LENGTH + envelopeEncoder.encodedLength();
        pc.submitNanos = System.nanoTime();
        pc.deadlineNanos = pc.submitNanos + config.retryBackoffNs();

        pending.put(pc.commandIdLo, pc);
        submitted++;
        offerUntilSent(pc);
        return pc.commandIdLo;
    }

    /**
     * Drives egress delivery, time-based retransmission, idle keepalives, and
     * session recovery. Call in a loop.
     *
     * @return an opaque work count (positive when progress was made).
     */
    public int poll() {
        final long now = System.nanoTime();
        if (sessionLost) {
            reconnect(now);
            return 0;
        }
        int work = cluster.pollEgress();
        work += retransmitDue(now);
        retransmitAll = false;

        if (config.keepaliveIntervalNs() > 0 && (now - nextKeepaliveNanos) >= 0) {
            sendKeepalive(now);
            work++;
        }
        return work;
    }

    // Re-offers every due command in submission (clientSeq) order. Pool-index
    // order diverges from submission order once slots are reused, and a leader
    // change or reconnect makes every in-flight command due at once; dependent
    // commands (a place followed by its cancel) must not be delivered reversed.
    private int retransmitDue(final long now) {
        // Scan the preallocated pool rather than the map's value iterator so a
        // poll neither allocates nor risks concurrent modification when a result
        // callback recycles an entry mid-scan.
        int dueCount = 0;
        for (int i = 0; i < pool.length; i++) {
            final PendingCommand pc = pool[i];
            if (pc.inUse && (retransmitAll || (now - pc.deadlineNanos) >= 0)) {
                dueOrder[dueCount++] = i;
            }
        }
        // Insertion sort: the due set is normally tiny, and the failover burst
        // that fills it is rare and already off the fast path.
        for (int i = 1; i < dueCount; i++) {
            final int idx = dueOrder[i];
            final long seq = pool[idx].clientSeq;
            int j = i - 1;
            while (j >= 0 && pool[dueOrder[j]].clientSeq > seq) {
                dueOrder[j + 1] = dueOrder[j];
                j--;
            }
            dueOrder[j + 1] = idx;
        }
        int work = 0;
        for (int d = 0; d < dueCount; d++) {
            final PendingCommand pc = pool[dueOrder[d]];
            // An expiry callback earlier in this pass may have recycled and
            // re-submitted the slot; re-check eligibility before re-offering.
            if (!pc.inUse || (!retransmitAll && (now - pc.deadlineNanos) < 0)) {
                continue;
            }
            if (config.maxRetries() > 0 && pc.retries >= config.maxRetries()) {
                expire(pc);
                continue;
            }
            // A resend is only safe while the engine's per-client dedup window
            // can still match the original (clientId, clientSeq); once more
            // than dedupWindow commands have been submitted since, retrying
            // would apply the command a second time. Expire instead of risking
            // a double-apply: the command's outcome is unrecoverable.
            if (config.dedupWindow() > 0 && nextClientSeq - pc.clientSeq > config.dedupWindow()) {
                expire(pc);
                continue;
            }
            offer(pc);
            if (pc == queuedUnsent && pc.deadlineNanos != 0L) {
                // The driver accepted the queued head; later submits may proceed.
                queuedUnsent = null;
            }
            pc.retries++;
            if (firstRetransmitIdLo == Long.MIN_VALUE) {
                firstRetransmitIdLo = pc.commandIdLo;
            }
            retransmits++;
            work++;
        }
        return work;
    }

    private void sendKeepalive(final long now) {
        final long commandIdLo;
        try {
            commandIdLo = submitKeepalive();
        } catch (final BackpressureException e) {
            // A queued head is still unsent (or the reserved slot is gone); try
            // again next cycle rather than reordering against it.
            nextKeepaliveNanos = now + config.keepaliveIntervalNs();
            return;
        }
        final PendingCommand pc = pending.get(commandIdLo);
        if (pc != null) {
            pc.keepalive = true;
        }
        nextKeepaliveNanos = now + config.keepaliveIntervalNs();
    }

    // Uses the slot reserved by windowFullForSubmit so a fully-saturated window
    // cannot starve the session-keepalive NOP.
    private long submitKeepalive() {
        if (queuedUnsent != null || freeTop == 0) {
            backpressureEvents++;
            throw new BackpressureException("in-flight window full: " + config.maxInFlight());
        }
        return encodeAndSubmitInner(
                OrderCommandType.NOP,
                0L,
                CommandEnvelopeEncoder.currencyNullValue(),
                CommandEnvelopeEncoder.balanceAmountNullValue());
    }

    private void reconnect(final long now) {
        if ((now - nextReconnectNanos) < 0) {
            return;
        }
        try {
            cluster.close();
        } catch (final RuntimeException ignored) {
            // Best-effort teardown of the lost session.
        }
        try {
            // Use the configured message timeout, matching the initial connect
            // (a reconnect must not hardcode a shorter, easier-to-fail budget).
            cluster = AeronCluster.connect(clusterContext(config.messageTimeoutNs()));
            sessionLost = false;
            retransmitAll = true;
            reconnects++;
            nextKeepaliveNanos = System.nanoTime() + config.keepaliveIntervalNs();
        } catch (final RuntimeException e) {
            // Counted so repeated reconnect failures are observable; the next
            // attempt is scheduled after a fixed backoff.
            reconnectFailures++;
            nextReconnectNanos = now + TimeUnit.SECONDS.toNanos(1);
        }
    }

    private void expire(final PendingCommand pc) {
        pending.remove(pc.commandIdLo);
        if (pc == queuedUnsent) {
            queuedUnsent = null;
        }
        if (!pc.keepalive) {
            handler.onExpired(pc.commandIdHi, pc.commandIdLo);
        }
        if (firstExpiredIdLo == Long.MIN_VALUE) {
            firstExpiredIdLo = pc.commandIdLo;
        }
        expired++;
        release(pc);
    }

    private long lastOfferResult;

    /**
     * Offers a command and retries transient negative offer results (backpressure,
     * admin action, not-yet-connected) in place up to the retry backoff, so
     * submission order equals cluster delivery order: if submit returned while the
     * command sat pending, a later retry would land at the end of the ingress
     * queue and be processed after subsequent commands, silently reordering the
     * caller's sequence (a cancel could then arrive before the order it targets).
     * Still unsent after the bound, the command becomes the queued-unsent head:
     * poll() re-offers it before any later command is accepted.
     */
    private void offerUntilSent(final PendingCommand pc) {
        final long deadline = System.nanoTime() + config.retryBackoffNs();
        for (; ; ) {
            if (sessionLost) {
                backpressureEvents++;
                queueUnsent(pc);
                return;
            }
            final long result = cluster.offer(pc.buffer, 0, pc.length);
            lastOfferResult = result;
            if (result >= 0) {
                pc.deadlineNanos = System.nanoTime() + config.retryBackoffNs();
                // Any ingress traffic resets the cluster's session timer.
                nextKeepaliveNanos = System.nanoTime() + config.keepaliveIntervalNs();
                return;
            }
            backpressureEvents++;
            if (firstNegativeOfferResult == 0L) {
                firstNegativeOfferResult = result;
            }
            if (result == Publication.CLOSED) {
                sessionLost = true;
                queueUnsent(pc);
                return;
            }
            if (System.nanoTime() >= deadline) {
                queueUnsent(pc);
                return;
            }
            Thread.onSpinWait();
        }
    }

    private void queueUnsent(final PendingCommand pc) {
        pc.deadlineNanos = 0L;
        if (queuedUnsent == null) {
            queuedUnsent = pc;
        }
    }

    private void offer(final PendingCommand pc) {
        if (sessionLost) {
            backpressureEvents++;
            return;
        }
        final long result = cluster.offer(pc.buffer, 0, pc.length);
        lastOfferResult = result;
        if (result < 0) {
            backpressureEvents++;
            if (firstNegativeOfferResult == 0L) {
                firstNegativeOfferResult = result;
            }
            if (result == Publication.CLOSED) {
                sessionLost = true;
            } else {
                // A backpressured or not-yet-connected offer must be retried on
                // the next poll, not after the full retry backoff: at high
                // throughput the engine's per-client dedup window rolls in a
                // few milliseconds, so waiting would make the retry unsafe.
                pc.deadlineNanos = 0L;
            }
        } else {
            pc.deadlineNanos = System.nanoTime() + config.retryBackoffNs();
            // Any ingress traffic resets the cluster's session timer.
            nextKeepaliveNanos = System.nanoTime() + config.keepaliveIntervalNs();
        }
    }

    /** The raw result of the most recent ingress offer (diagnostics). */
    public long lastOfferResult() {
        return lastOfferResult;
    }

    /** The first negative ingress offer result (0 when every offer succeeded). */
    public long firstNegativeOfferResult() {
        return firstNegativeOfferResult;
    }

    private void release(final PendingCommand pc) {
        pc.reset();
        freeStack[freeTop++] = pc.poolIndex;
    }

    @Override
    public void onMessage(
            final long clusterSessionId,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {
        headerDecoder.wrap(buffer, offset);
        // A truncated frame (or one whose header lies about its block length)
        // must be dropped rather than letting a decoder read past the buffer and
        // throw out of the egress poll loop.
        if (length < MessageHeaderDecoder.ENCODED_LENGTH + headerDecoder.blockLength()) {
            return;
        }
        final int templateId = headerDecoder.templateId();
        if (templateId == TradeEventDecoder.TEMPLATE_ID) {
            tradeDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());
            tradeListener.onTrade(
                    tradeDecoder.commandIdHi(),
                    tradeDecoder.commandIdLo(),
                    eventIndex(
                            tradeDecoder.eventIndexExt(),
                            TradeEventDecoder.eventIndexExtNullValue(),
                            tradeDecoder.eventIndex()),
                    tradeDecoder.symbolId(),
                    tradeDecoder.makerOrderId(),
                    tradeDecoder.makerUid(),
                    tradeDecoder.takerUid(),
                    tradeDecoder.price(),
                    tradeDecoder.size(),
                    tradeDecoder.makerOrderCompleted() != 0);

            final long idLo = tradeDecoder.commandIdLo();
            if (tradeGroup.fillCount() > 0 && tradeGroup.commandIdLo() != idLo) {
                flushTradeGroup();
            }
            if (tradeGroup.fillCount() == 0) {
                tradeGroup.begin(tradeDecoder.commandIdHi(), idLo, tradeDecoder.symbolId(), tradeDecoder.takerUid());
            }
            tradeGroup.addFill(
                    tradeDecoder.makerOrderId(),
                    tradeDecoder.makerUid(),
                    tradeDecoder.price(),
                    tradeDecoder.size(),
                    tradeDecoder.makerOrderCompleted() != 0);
            countDownEventStream(idLo);
            return;
        }
        if (templateId == ReduceEventDecoder.TEMPLATE_ID) {
            reduceDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());
            // A reduce never shares a command with trades, so any open group is stale.
            flushTradeGroup();
            reduceListener.onReduce(
                    reduceDecoder.commandIdHi(),
                    reduceDecoder.commandIdLo(),
                    eventIndex(
                            reduceDecoder.eventIndexExt(),
                            ReduceEventDecoder.eventIndexExtNullValue(),
                            reduceDecoder.eventIndex()),
                    reduceDecoder.symbolId(),
                    reduceDecoder.orderId(),
                    reduceDecoder.uid(),
                    reduceDecoder.reducedBy(),
                    reduceDecoder.price(),
                    reduceDecoder.orderCompleted() != 0);
            countDownEventStream(reduceDecoder.commandIdLo());
            return;
        }
        if (templateId == RejectEventDecoder.TEMPLATE_ID) {
            rejectDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());
            // Within one command the reject trails the fills, so the group is complete.
            flushTradeGroup();
            rejectListener.onReject(
                    rejectDecoder.commandIdHi(),
                    rejectDecoder.commandIdLo(),
                    eventIndex(
                            rejectDecoder.eventIndexExt(),
                            RejectEventDecoder.eventIndexExtNullValue(),
                            rejectDecoder.eventIndex()),
                    rejectDecoder.symbolId(),
                    rejectDecoder.orderId(),
                    rejectDecoder.uid(),
                    rejectDecoder.rejectedSize(),
                    rejectDecoder.price());
            countDownEventStream(rejectDecoder.commandIdLo());
            return;
        }
        if (templateId == L2MarketDataDecoder.TEMPLATE_ID) {
            l2Decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());
            // The snapshot trails the command's matcher events.
            flushTradeGroup();
            snapshot.begin(l2Decoder.commandIdHi(), l2Decoder.commandIdLo(), l2Decoder.symbolId());
            for (final L2MarketDataDecoder.AsksDecoder asks = l2Decoder.asks(); asks.hasNext(); ) {
                asks.next();
                snapshot.addAsk(asks.price(), asks.size(), asks.orders());
            }
            for (final L2MarketDataDecoder.BidsDecoder bids = l2Decoder.bids(); bids.hasNext(); ) {
                bids.next();
                snapshot.addBid(bids.price(), bids.size(), bids.orders());
            }
            orderBookListener.onOrderBook(snapshot);
            return;
        }
        if (templateId != CommandResultDecoder.TEMPLATE_ID) {
            return;
        }
        resultDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(),
                headerDecoder.version());

        // Results precede their own events, so any open group is from an older command.
        flushTradeGroup();
        final long commandIdLo = resultDecoder.commandIdLo();
        final long announcedExt = resultDecoder.eventCountExt();
        final int announced;
        if (announcedExt != CommandResultDecoder.eventCountExtNullValue()) {
            announced = (int) announcedExt;
        } else {
            final int legacy = resultDecoder.eventCount();
            announced = (legacy == CommandResultDecoder.eventCountNullValue()) ? -1 : legacy;
        }
        eventStreamIdLo = commandIdLo;
        eventStreamRemaining = announced;
        final PendingCommand pc = pending.remove(commandIdLo);
        final boolean hasUid = resultDecoder.uid() != CommandResultDecoder.uidNullValue();
        final boolean hasOrderId = resultDecoder.orderId() != CommandResultDecoder.orderIdNullValue();
        final boolean hasFilledSize = resultDecoder.filledSize() != CommandResultDecoder.filledSizeNullValue();

        if (pc != null) {
            final boolean keepalive = pc.keepalive;
            if (!keepalive) {
                final long elapsedNs = System.nanoTime() - pc.submitNanos;
                // Clamp so a result arriving after a long outage cannot throw out of
                // the poll loop (Histogram rejects values above highestTrackableValue).
                latencyHistogram.recordValue(Math.min(elapsedNs, latencyHistogram.getHighestTrackableValue()));
            }
            release(pc);
            if (keepalive) {
                keepalives++;
                return;
            }
            completed++;
        }

        handler.onResult(
                resultDecoder.commandIdHi(),
                commandIdLo,
                resultDecoder.resultCode(),
                resultDecoder.uid(),
                hasUid,
                resultDecoder.orderId(),
                hasOrderId,
                resultDecoder.filledSize(),
                hasFilledSize);
    }

    /** Delivers the open trade group (when non-empty) and clears it for reuse. */
    private void flushTradeGroup() {
        if (tradeGroup.fillCount() > 0) {
            tradeGroupListener.onTradeGroup(tradeGroup);
            tradeGroup.clear();
        }
    }

    /** Tracks the announced event stream; the last expected frame flushes the group. */
    private void countDownEventStream(final long idLo) {
        if (eventStreamRemaining > 0 && idLo == eventStreamIdLo) {
            eventStreamRemaining--;
            if (eventStreamRemaining == 0) {
                flushTradeGroup();
            }
        }
    }

    // Prefers the uint32 index extension added in v5; falls back to the legacy
    // uint16 index for frames recorded before it (whose index never wrapped).
    private static int eventIndex(final long ext, final long extNullValue, final int legacy) {
        return ext == extNullValue ? legacy : (int) ext;
    }

    @Override
    public void onSessionEvent(
            final long clusterSessionId,
            final long leadershipTermId,
            final long eventCorrelationId,
            final int leaderMemberId,
            final EventCode eventCode,
            final String detail) {
        if (eventCode == EventCode.CLOSED || eventCode == EventCode.ERROR) {
            sessionLost = true;
        }
    }

    @Override
    public void onNewLeader(
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final String ingressEndpoints) {
        this.leaderMemberId = leaderMemberId;
        this.leaderChanges++;
        this.retransmitAll = true;
    }

    public int pendingCount() {
        return pending.size();
    }

    public long submitted() {
        return submitted;
    }

    public long completed() {
        return completed;
    }

    /** Commands abandoned after exhausting {@code maxRetries}; each was reported via {@link ResultHandler#onExpired}. */
    public long expired() {
        return expired;
    }

    /** Commands re-offered to the cluster because their retry deadline passed without a result. */
    public long retransmits() {
        return retransmits;
    }

    /** The command id of the first retransmitted command, or {@link Long#MIN_VALUE} when none were. */
    public long firstRetransmitIdLo() {
        return firstRetransmitIdLo;
    }

    /** The command id of the first expired command, or {@link Long#MIN_VALUE} when none were. */
    public long firstExpiredIdLo() {
        return firstExpiredIdLo;
    }

    public long backpressureEvents() {
        return backpressureEvents;
    }

    /** Idle NOP keepalives submitted to hold the cluster session open. */
    public long keepalives() {
        return keepalives;
    }

    /** Sessions re-established after a loss (cluster restart, CLOSED / ERROR event). */
    public int reconnects() {
        return reconnects;
    }

    /** Reconnect attempts that failed to establish a session (swallowed errors, now counted). */
    public int reconnectFailures() {
        return reconnectFailures;
    }

    public int leaderChanges() {
        return leaderChanges;
    }

    public int leaderMemberId() {
        final int fromCluster = cluster.leaderMemberId();
        return fromCluster >= 0 ? fromCluster : leaderMemberId;
    }

    /** End-to-end latency (submit to result) in nanoseconds; read from the poll thread. */
    public Histogram latencyHistogram() {
        return latencyHistogram;
    }

    @Override
    public void close() {
        cluster.close();
        if (ownMediaDriver != null) {
            ownMediaDriver.close();
        }
    }
}
