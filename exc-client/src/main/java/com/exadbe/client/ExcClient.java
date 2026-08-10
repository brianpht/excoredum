package com.exadbe.client;

import com.exadbe.client.config.ClientConfig;
import com.exadbe.protocol.CommandEnvelopeEncoder;
import com.exadbe.protocol.CommandResultDecoder;
import com.exadbe.protocol.L2MarketDataDecoder;
import com.exadbe.protocol.MessageHeaderDecoder;
import com.exadbe.protocol.MessageHeaderEncoder;
import com.exadbe.protocol.OrderAction;
import com.exadbe.protocol.OrderCommandType;
import com.exadbe.protocol.OrderType;
import com.exadbe.protocol.ReduceEventDecoder;
import com.exadbe.protocol.RejectEventDecoder;
import com.exadbe.protocol.TradeEventDecoder;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * Edge-side client for the excoredum core. Adds, on top of a raw Aeron cluster
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
public final class ExcClient implements EgressListener, AutoCloseable {

    private static final float LOAD_FACTOR = 0.65f;

    private final ClientConfig config;
    private final ResultHandler handler;
    private final AeronCluster cluster;
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

    private final Histogram latencyHistogram = new Histogram(3_600_000_000_000L, 3);

    private long nextClientSeq;
    private long nextCommandIdLo = 1L;

    private long submitted;
    private long completed;
    private long expired;
    private long backpressureEvents;
    private int leaderChanges;
    private int leaderMemberId = -1;
    private boolean retransmitAll;

    public ExcClient(final ClientConfig config, final ResultHandler handler) {
        this.config = config;
        this.handler = handler;
        // Size the pending map so it never rehashes while the in-flight window
        // (bounded by maxInFlight) is populated.
        this.pending = new Long2ObjectHashMap<>(Math.max(16, config.maxInFlight() * 2), LOAD_FACTOR);
        this.pool = new PendingCommand[config.maxInFlight()];
        this.freeStack = new int[config.maxInFlight()];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new PendingCommand(i);
            freeStack[i] = pool.length - 1 - i;
        }
        this.freeTop = pool.length;

        MediaDriver embedded = null;
        String aeronDir = config.aeronDirectoryName();
        if (aeronDir == null) {
            embedded = MediaDriver.launchEmbedded(new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true));
            aeronDir = embedded.aeronDirectoryName();
        }
        this.ownMediaDriver = embedded;

        try {
            this.cluster = AeronCluster.connect(new AeronCluster.Context()
                    .egressListener(this)
                    .aeronDirectoryName(aeronDir)
                    .ingressChannel("aeron:udp")
                    .egressChannel(config.egressChannel())
                    .messageTimeoutNs(config.messageTimeoutNs())
                    .ingressEndpoints(config.ingressEndpoints()));
        } catch (final RuntimeException e) {
            if (embedded != null) {
                embedded.close();
            }
            throw e;
        }
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
        if (freeTop == 0) {
            backpressureEvents++;
            throw new BackpressureException("in-flight window full: " + config.maxInFlight());
        }
        final PendingCommand pc = pool[freeStack[--freeTop]];
        pc.inUse = true;
        pc.retries = 0;
        pc.commandIdHi = config.clientId();
        pc.commandIdLo = nextCommandIdLo++;

        envelopeEncoder
                .wrapAndApplyHeader(pc.buffer, 0, headerEncoder)
                .clientId(config.clientId())
                .clientSeq(nextClientSeq++)
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
        offer(pc);
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
            final long uid) {
        return submitOrder(
                OrderCommandType.PLACE_ORDER,
                symbolId,
                orderId,
                uid,
                ask ? OrderAction.ASK : OrderAction.BID,
                OrderType.GTC,
                price,
                reserveBidPrice,
                size);
    }

    /** Submits an IOC limit order. */
    public long placeIoc(
            final int symbolId,
            final long orderId,
            final boolean ask,
            final long price,
            final long size,
            final long uid) {
        return submitOrder(
                OrderCommandType.PLACE_ORDER,
                symbolId,
                orderId,
                uid,
                ask ? OrderAction.ASK : OrderAction.BID,
                OrderType.IOC,
                price,
                CommandEnvelopeEncoder.reserveBidPriceNullValue(),
                size);
    }

    /** Submits a fill-or-kill budget order ({@code budget} is the total price limit). */
    public long placeFokBudget(
            final int symbolId,
            final long orderId,
            final boolean ask,
            final long budget,
            final long size,
            final long uid) {
        return submitOrder(
                OrderCommandType.PLACE_ORDER,
                symbolId,
                orderId,
                uid,
                ask ? OrderAction.ASK : OrderAction.BID,
                OrderType.FOK_BUDGET,
                budget,
                CommandEnvelopeEncoder.reserveBidPriceNullValue(),
                size);
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
                CommandEnvelopeEncoder.sizeNullValue());
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
                CommandEnvelopeEncoder.sizeNullValue());
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
                size);
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
                CommandEnvelopeEncoder.sizeNullValue());
    }

    private long encodeAndSubmit(
            final OrderCommandType type, final long uid, final int currency, final long balanceAmount) {
        if (freeTop == 0) {
            backpressureEvents++;
            throw new BackpressureException("in-flight window full: " + config.maxInFlight());
        }

        final PendingCommand pc = pool[freeStack[--freeTop]];
        pc.inUse = true;
        pc.retries = 0;
        pc.commandIdHi = config.clientId();
        pc.commandIdLo = nextCommandIdLo++;

        envelopeEncoder
                .wrapAndApplyHeader(pc.buffer, 0, headerEncoder)
                .clientId(config.clientId())
                .clientSeq(nextClientSeq++)
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
        offer(pc);
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
            final long size) {
        if (freeTop == 0) {
            backpressureEvents++;
            throw new BackpressureException("in-flight window full: " + config.maxInFlight());
        }

        final PendingCommand pc = pool[freeStack[--freeTop]];
        pc.inUse = true;
        pc.retries = 0;
        pc.commandIdHi = config.clientId();
        pc.commandIdLo = nextCommandIdLo++;

        envelopeEncoder
                .wrapAndApplyHeader(pc.buffer, 0, headerEncoder)
                .clientId(config.clientId())
                .clientSeq(nextClientSeq++)
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
                .userCookie(CommandEnvelopeEncoder.userCookieNullValue())
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
        offer(pc);
        return pc.commandIdLo;
    }

    /**
     * Drives egress delivery and time-based retransmission. Call in a loop.
     *
     * @return an opaque work count (positive when progress was made).
     */
    public int poll() {
        int work = cluster.pollEgress();
        final long now = System.nanoTime();

        // Scan the preallocated pool rather than the map's value iterator so a
        // poll neither allocates nor risks concurrent modification when a result
        // callback recycles an entry mid-scan.
        for (int i = 0; i < pool.length; i++) {
            final PendingCommand pc = pool[i];
            if (!pc.inUse) {
                continue;
            }
            final boolean due = retransmitAll || (now - pc.deadlineNanos) >= 0;
            if (!due) {
                continue;
            }
            if (config.maxRetries() > 0 && pc.retries >= config.maxRetries()) {
                expire(pc);
                continue;
            }
            offer(pc);
            pc.retries++;
            pc.deadlineNanos = now + config.retryBackoffNs();
            work++;
        }
        retransmitAll = false;
        return work;
    }

    private void expire(final PendingCommand pc) {
        pending.remove(pc.commandIdLo);
        handler.onExpired(pc.commandIdHi, pc.commandIdLo);
        expired++;
        release(pc);
    }

    private void offer(final PendingCommand pc) {
        final long result = cluster.offer(pc.buffer, 0, pc.length);
        if (result < 0) {
            backpressureEvents++;
        }
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
                    tradeDecoder.eventIndex(),
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
                    reduceDecoder.eventIndex(),
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
                    rejectDecoder.eventIndex(),
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
        final int announced = resultDecoder.eventCount();
        eventStreamIdLo = commandIdLo;
        eventStreamRemaining = (announced == CommandResultDecoder.eventCountNullValue()) ? -1 : announced;
        final PendingCommand pc = pending.remove(commandIdLo);
        final boolean hasUid = resultDecoder.uid() != CommandResultDecoder.uidNullValue();
        final boolean hasOrderId = resultDecoder.orderId() != CommandResultDecoder.orderIdNullValue();
        final boolean hasFilledSize = resultDecoder.filledSize() != CommandResultDecoder.filledSizeNullValue();

        if (pc != null) {
            final long elapsedNs = System.nanoTime() - pc.submitNanos;
            // Clamp so a result arriving after a long outage cannot throw out of
            // the poll loop (Histogram rejects values above highestTrackableValue).
            latencyHistogram.recordValue(Math.min(elapsedNs, latencyHistogram.getHighestTrackableValue()));
            release(pc);
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

    public long backpressureEvents() {
        return backpressureEvents;
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
