package com.exadbe.engine;

import com.exadbe.collections.AccountStore;
import com.exadbe.collections.DedupRing;
import com.exadbe.collections.DedupTable;
import com.exadbe.config.CoreConfig;
import com.exadbe.core.CommandOutcome;
import com.exadbe.engine.handlers.AddSymbolHandler;
import com.exadbe.engine.handlers.AddUserHandler;
import com.exadbe.engine.handlers.BalanceAdjustmentHandler;
import com.exadbe.engine.handlers.ResumeUserHandler;
import com.exadbe.engine.handlers.SuspendUserHandler;
import com.exadbe.engine.orderbook.L2View;
import com.exadbe.engine.orderbook.OrderBookNaive;
import com.exadbe.engine.orderbook.OrderNodePool;
import com.exadbe.engine.orderbook.PriceBucketPool;
import com.exadbe.engine.risk.DirectExchangeRisk;
import com.exadbe.engine.risk.SymbolSpec;
import com.exadbe.engine.risk.SymbolSpecStore;
import com.exadbe.protocol.CommandEnvelopeDecoder;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.protocol.OrderAction;
import com.exadbe.protocol.OrderType;
import com.exadbe.snapshot.SnapshotManager;
import com.exadbe.telemetry.CoreMetrics;
import com.exadbe.util.Amounts;
import java.util.Arrays;
import org.agrona.collections.Int2ObjectHashMap;

/**
 * The deterministic state machine: idempotent dispatch of commands over the
 * account and (from phase 2) order-book state. Intentionally free of any Aeron
 * dependency so it can be driven directly in unit and replay tests.
 *
 * <p>Single-writer: all methods must be invoked from one thread. No clock, no
 * randomness, no external I/O.
 */
public final class MatchingEngine {

    private final AccountStore accounts;
    private final DedupTable dedup;
    private final CoreMetrics metrics;

    private final AddUserHandler addUserHandler;
    private final BalanceAdjustmentHandler balanceAdjustmentHandler;
    private final AddSymbolHandler addSymbolHandler;
    private final SuspendUserHandler suspendUserHandler;
    private final ResumeUserHandler resumeUserHandler;

    private final SymbolSpecStore symbols = new SymbolSpecStore();
    private final DirectExchangeRisk risk;
    private final Int2ObjectHashMap<OrderBookNaive> books = new Int2ObjectHashMap<>();
    private final OrderNodePool orderPool;
    private final PriceBucketPool bucketPool;
    private final L2View l2;

    private int[] symbolScratch = new int[0];
    private long lastPoolAllocations;
    private long lastBucketPoolAllocations;

    public MatchingEngine(final CoreConfig config, final CoreMetrics metrics) {
        this.accounts = new AccountStore(config.accountCapacity());
        this.dedup = new DedupTable(config.dedupClientCapacity(), config.dedupWindow());
        this.metrics = metrics;
        this.addUserHandler = new AddUserHandler(accounts);
        this.balanceAdjustmentHandler = new BalanceAdjustmentHandler(accounts);
        this.addSymbolHandler = new AddSymbolHandler(symbols);
        this.suspendUserHandler = new SuspendUserHandler(accounts);
        this.resumeUserHandler = new ResumeUserHandler(accounts);
        this.risk = new DirectExchangeRisk(accounts);
        this.orderPool = new OrderNodePool(config.orderPoolCapacity());
        this.bucketPool = new PriceBucketPool(config.priceBucketCapacity());
        this.l2 = new L2View(config.l2MaxLevels());
    }

    /**
     * Processes one decoded command and populates {@code out}.
     *
     * @param timestamp leader-assigned time; the only permitted time source
     * @return {@code true} if this was a duplicate (cached result returned,
     *     command not re-applied), {@code false} if freshly applied.
     */
    public boolean process(final CommandEnvelopeDecoder cmd, final long timestamp, final CommandOutcome out) {
        final long clientId = cmd.clientId();
        final long clientSeq = cmd.clientSeq();

        if (clientSeq == DedupRing.EMPTY) {
            // The all-ones uint64 is wire-legal but collides with the ring's
            // unoccupied-slot sentinel: it could never be deduplicated and storing
            // it would erase a colliding sequence's record. Reject without caching.
            out.reset(cmd.commandIdHi(), cmd.commandIdLo());
            out.resultCode(CommandResultCode.INVALID_AMOUNT);
            metrics.onCommandProcessed();
            return false;
        }

        final DedupRing ring = dedup.ringFor(clientId);
        if (ring != null && ring.contains(clientSeq)) {
            out.set(
                    ring.commandIdHi(clientSeq),
                    ring.commandIdLo(clientSeq),
                    CommandResultCode.get(ring.resultCode(clientSeq)),
                    ring.uid(clientSeq),
                    ring.hasUid(clientSeq),
                    ring.orderId(clientSeq),
                    ring.hasOrderId(clientSeq),
                    ring.filledSize(clientSeq),
                    ring.hasFilledSize(clientSeq));
            metrics.onDuplicate();
            return true;
        }

        final long idHi = cmd.commandIdHi();
        final long idLo = cmd.commandIdLo();
        out.reset(idHi, idLo);
        dispatch(cmd, timestamp, out);
        if (out.grewEventBuffer()) {
            metrics.onEventBufferOverflow();
        }
        if (orderPool.allocations() != lastPoolAllocations) {
            lastPoolAllocations = orderPool.allocations();
            metrics.onOrderPoolExhausted();
        }
        if (bucketPool.allocations() != lastBucketPoolAllocations) {
            lastBucketPoolAllocations = bucketPool.allocations();
            metrics.onPriceBucketPoolExhausted();
        }

        dedup.store(
                clientId,
                clientSeq,
                idHi,
                idLo,
                out.resultCode().value(),
                out.uid(),
                out.hasUid(),
                out.orderId(),
                out.hasOrderId(),
                out.filledSize(),
                out.hasFilledSize());
        metrics.onCommandProcessed();
        return false;
    }

    private void dispatch(final CommandEnvelopeDecoder cmd, final long timestamp, final CommandOutcome out) {
        switch (cmd.commandType()) {
            case ADD_USER -> addUserHandler.handle(cmd.uid(), out);
            case BALANCE_ADJUSTMENT -> balanceAdjustmentHandler.handle(
                    cmd.uid(), cmd.currency(), cmd.balanceAmount(), out);
            case ADD_SYMBOL -> addSymbolHandler.handle(
                    cmd.symbolId(),
                    cmd.baseCurrency(),
                    cmd.quoteCurrency(),
                    cmd.baseScaleK(),
                    cmd.quoteScaleK(),
                    feeOrZero(cmd.takerFee()),
                    feeOrZero(cmd.makerFee()),
                    out);
            case PLACE_ORDER -> handlePlace(cmd, timestamp, out);
            case CANCEL_ORDER -> handleCancel(cmd, out);
            case MOVE_ORDER -> handleMove(cmd, out);
            case REDUCE_ORDER -> handleReduce(cmd, out);
            case ORDER_BOOK_REQUEST -> handleOrderBookRequest(cmd, out);
            case SUSPEND_USER -> suspendUserHandler.handle(cmd.uid(), out);
            case RESUME_USER -> resumeUserHandler.handle(cmd.uid(), out);
            case RESET -> handleReset(out);
            case NOP -> handleNop(cmd, out);
            default -> unsupported(cmd, out);
        }
    }

    private void handlePlace(final CommandEnvelopeDecoder cmd, final long timestamp, final CommandOutcome out) {
        final int symbolId = cmd.symbolId();
        final long orderId = cmd.orderId();
        final long uid = cmd.uid();
        out.uid(uid);
        out.orderId(orderId);

        final SymbolSpec spec = symbols.get(symbolId);
        if (spec == null) {
            out.resultCode(CommandResultCode.INVALID_SYMBOL);
            return;
        }
        if (uid == DirectExchangeRisk.FEE_ACCOUNT_UID) {
            out.resultCode(CommandResultCode.USER_NOT_FOUND);
            return;
        }
        if (!accounts.userExists(uid)) {
            out.resultCode(CommandResultCode.USER_NOT_FOUND);
            return;
        }
        if (accounts.isSuspended(uid)) {
            out.resultCode(CommandResultCode.USER_SUSPENDED);
            return;
        }

        final boolean ask = cmd.action() == OrderAction.ASK;
        final OrderType type = cmd.orderType();
        final long size = cmd.size();
        final long price = cmd.price();
        final long reserveBidPrice = cmd.reserveBidPrice();

        // Non-positive operands would mint money: a negative size produces a
        // negative hold, and reserving a negative amount credits the balance.
        // The price field doubles as the budget for FOK-BUDGET orders.
        if (size <= 0L || price <= 0L) {
            out.resultCode(CommandResultCode.INVALID_AMOUNT);
            return;
        }
        // Every balance credit this order can ever produce is overflow-checked up
        // front (before any reservation), so the match and settle paths below can
        // never fail arithmetically for this order's own operands.
        if (Amounts.mulOverflows(size, spec.baseScaleK())) {
            out.resultCode(CommandResultCode.OVERFLOW);
            return;
        }

        final int holdCurrency;
        final long holdAmount;
        if (ask) {
            // An ask must fill for at least its taker fee, or proceeds could go
            // negative. Overflow-safe: with positive operands an overflowing
            // product exceeds any fitting fee, so the floor holds.
            if (!Amounts.mulOverflows(price, spec.quoteScaleK()) && price * spec.quoteScaleK() < spec.takerFee()) {
                out.resultCode(CommandResultCode.RISK_ASK_PRICE_LOWER_THAN_FEE);
                return;
            }
            // Worst-case sell proceeds and fee debit, checked before the hold.
            if (Amounts.mulOverflows(size, price)
                    || Amounts.mulOverflows(size * price, spec.quoteScaleK())
                    || Amounts.mulOverflows(spec.takerFee(), size)) {
                out.resultCode(CommandResultCode.OVERFLOW);
                return;
            }
            holdCurrency = spec.baseCurrency();
            holdAmount = DirectExchangeRisk.askHold(spec, size);
        } else if (type == OrderType.FOK_BUDGET) {
            if (Amounts.mulOverflows(price, spec.quoteScaleK())
                    || Amounts.mulOverflows(size, spec.takerFee())
                    || Amounts.addOverflows(price * spec.quoteScaleK(), size * spec.takerFee())) {
                out.resultCode(CommandResultCode.OVERFLOW);
                return;
            }
            holdCurrency = spec.quoteCurrency();
            holdAmount = DirectExchangeRisk.bidBudgetHold(spec, size, price);
        } else {
            if (reserveBidPrice < price) {
                out.resultCode(CommandResultCode.RISK_INVALID_RESERVE_PRICE);
                return;
            }
            if (Amounts.mulOverflows(reserveBidPrice, spec.quoteScaleK())
                    || Amounts.addOverflows(reserveBidPrice * spec.quoteScaleK(), spec.takerFee())
                    || Amounts.mulOverflows(size, reserveBidPrice * spec.quoteScaleK() + spec.takerFee())) {
                out.resultCode(CommandResultCode.OVERFLOW);
                return;
            }
            holdCurrency = spec.quoteCurrency();
            holdAmount = DirectExchangeRisk.bidHold(spec, size, reserveBidPrice);
        }

        if (!risk.reserve(uid, holdCurrency, holdAmount)) {
            out.resultCode(CommandResultCode.RISK_NSF);
            return;
        }

        final OrderBookNaive book = bookForCreate(symbolId);
        final long filled;
        switch (type) {
            case GTC -> filled = book.placeGtc(orderId, ask, price, size, reserveBidPrice, uid, timestamp, out);
            case IOC -> filled = book.matchIoc(orderId, ask, price, size, reserveBidPrice, uid, out);
            case FOK_BUDGET -> filled = book.matchFokBudget(orderId, ask, price, size, reserveBidPrice, uid, out);
            default -> {
                risk.release(uid, holdCurrency, holdAmount);
                out.resultCode(CommandResultCode.UNSUPPORTED_COMMAND);
                metrics.onUnsupportedCommand();
                return;
            }
        }

        final boolean fokBudget = type == OrderType.FOK_BUDGET;
        settleFills(spec, out, fokBudget, price);
        releaseRejects(spec, out, ask, fokBudget, price, reserveBidPrice);
        out.filledSize(filled);
        out.resultCode(CommandResultCode.SUCCESS);
    }

    private void handleCancel(final CommandEnvelopeDecoder cmd, final CommandOutcome out) {
        final long uid = cmd.uid();
        final long orderId = cmd.orderId();
        out.uid(uid);
        out.orderId(orderId);
        final int symbolId = cmd.symbolId();
        final OrderBookNaive book = books.get(symbolId);
        if (book == null) {
            out.resultCode(CommandResultCode.MATCHING_UNKNOWN_ORDER_ID);
            return;
        }
        final CommandResultCode code = book.cancel(orderId, uid, out);
        if (code == CommandResultCode.SUCCESS) {
            releaseReduces(symbols.get(symbolId), out);
        }
        out.resultCode(code);
    }

    private void handleMove(final CommandEnvelopeDecoder cmd, final CommandOutcome out) {
        final long uid = cmd.uid();
        final long orderId = cmd.orderId();
        out.uid(uid);
        out.orderId(orderId);
        final int symbolId = cmd.symbolId();
        final OrderBookNaive book = books.get(symbolId);
        if (book == null) {
            out.resultCode(CommandResultCode.MATCHING_UNKNOWN_ORDER_ID);
            return;
        }
        final CommandResultCode code = book.move(orderId, uid, cmd.price(), symbols.get(symbolId), out);
        if (code == CommandResultCode.SUCCESS) {
            settleFills(symbols.get(symbolId), out, false, 0L);
        }
        out.resultCode(code);
    }

    private void handleReduce(final CommandEnvelopeDecoder cmd, final CommandOutcome out) {
        final long uid = cmd.uid();
        final long orderId = cmd.orderId();
        out.uid(uid);
        out.orderId(orderId);
        final int symbolId = cmd.symbolId();
        final OrderBookNaive book = books.get(symbolId);
        if (book == null) {
            out.resultCode(CommandResultCode.MATCHING_UNKNOWN_ORDER_ID);
            return;
        }
        final CommandResultCode code = book.reduce(orderId, uid, cmd.size(), out);
        if (code == CommandResultCode.SUCCESS) {
            releaseReduces(symbols.get(symbolId), out);
        }
        out.resultCode(code);
    }

    // Settles maker fills at the actual price and the taker in aggregate, releasing
    // any over-reserved quote. Used by both PLACE and a marketable MOVE.
    //
    // Two passes: the first overflow-checks every credit and aggregate before any
    // balance is touched, so an arithmetic failure surfaces as OVERFLOW with all
    // balances intact. Placement validation bounds each order's own operands, so a
    // failure here requires a cross-maker aggregate beyond any realistic supply.
    private void settleFills(
            final SymbolSpec spec, final CommandOutcome out, final boolean fokBudget, final long budget) {
        if (!settleAmountsFit(spec, out, fokBudget, budget)) {
            out.resultCode(CommandResultCode.OVERFLOW);
            return;
        }
        long sizeSum = 0L;
        long sizePriceSum = 0L;
        long takerReserve = 0L;
        long takerUid = 0L;
        boolean takerBid = false;
        boolean anyTrade = false;
        final int n = out.eventCount();
        for (int i = 0; i < n; i++) {
            final CommandOutcome.EventRecord e = out.event(i);
            if (e.kind() != CommandOutcome.EventKind.TRADE) {
                continue;
            }
            risk.settleMaker(spec, e.makerBid(), e.makerUid(), e.makerReserveBidPrice(), e.price(), e.size());
            sizeSum += e.size();
            sizePriceSum += e.size() * e.price();
            takerUid = e.takerUid();
            takerBid = e.takerBid();
            takerReserve = e.takerReserveBidPrice();
            anyTrade = true;
        }
        if (!anyTrade) {
            return;
        }
        if (takerBid) {
            final long heldPriceSum = fokBudget ? budget : takerReserve * sizeSum;
            risk.settleTakerBuy(spec, takerUid, heldPriceSum, sizePriceSum, sizeSum);
        } else {
            risk.settleTakerSell(spec, takerUid, sizePriceSum, sizeSum);
        }
        risk.collectFee(spec, (spec.takerFee() + spec.makerFee()) * sizeSum);
    }

    // Overflow-checks every amount settleFills will credit, without mutating state.
    private boolean settleAmountsFit(
            final SymbolSpec spec, final CommandOutcome out, final boolean fokBudget, final long budget) {
        long sizeSum = 0L;
        long sizePriceSum = 0L;
        long takerReserve = 0L;
        boolean takerBid = false;
        boolean anyTrade = false;
        final int n = out.eventCount();
        for (int i = 0; i < n; i++) {
            final CommandOutcome.EventRecord e = out.event(i);
            if (e.kind() != CommandOutcome.EventKind.TRADE) {
                continue;
            }
            if (e.makerBid()) {
                // Base credit plus the quote refund; the per-lot refund stays
                // non-negative because price <= reserveBidPrice and
                // makerFee <= takerFee (both enforced at validation).
                if (Amounts.mulOverflows(e.size(), spec.baseScaleK())) {
                    return false;
                }
                if (Amounts.mulOverflows(e.makerReserveBidPrice() - e.price(), spec.quoteScaleK())) {
                    return false;
                }
                final long perLotRefund = (e.makerReserveBidPrice() - e.price()) * spec.quoteScaleK()
                        + (spec.takerFee() - spec.makerFee());
                if (Amounts.mulOverflows(e.size(), perLotRefund)) {
                    return false;
                }
            } else {
                if (Amounts.mulOverflows(e.size(), e.price())
                        || Amounts.mulOverflows(e.size() * e.price(), spec.quoteScaleK())
                        || Amounts.mulOverflows(spec.makerFee(), e.size())) {
                    return false;
                }
            }
            if (Amounts.mulOverflows(e.size(), e.price()) || Amounts.addOverflows(sizeSum, e.size())) {
                return false;
            }
            sizeSum += e.size();
            if (Amounts.addOverflows(sizePriceSum, e.size() * e.price())) {
                return false;
            }
            sizePriceSum += e.size() * e.price();
            takerBid = e.takerBid();
            takerReserve = e.takerReserveBidPrice();
            anyTrade = true;
        }
        if (!anyTrade) {
            return true;
        }
        if (takerBid) {
            final long heldPriceSum;
            if (fokBudget) {
                heldPriceSum = budget;
            } else {
                if (Amounts.mulOverflows(takerReserve, sizeSum)) {
                    return false;
                }
                heldPriceSum = takerReserve * sizeSum;
            }
            if (Amounts.mulOverflows(sizeSum, spec.baseScaleK())) {
                return false;
            }
            if (Amounts.mulOverflows(heldPriceSum - sizePriceSum, spec.quoteScaleK())) {
                return false;
            }
        } else {
            if (Amounts.mulOverflows(sizePriceSum, spec.quoteScaleK())
                    || Amounts.mulOverflows(spec.takerFee(), sizeSum)) {
                return false;
            }
        }
        if (Amounts.addOverflows(spec.takerFee(), spec.makerFee())) {
            return false;
        }
        return !Amounts.mulOverflows(spec.takerFee() + spec.makerFee(), sizeSum);
    }

    // Releases the taker's hold for any rejected (unmatched) size.
    private void releaseRejects(
            final SymbolSpec spec,
            final CommandOutcome out,
            final boolean takerAsk,
            final boolean fokBudget,
            final long budget,
            final long reserveBidPrice) {
        final int n = out.eventCount();
        for (int i = 0; i < n; i++) {
            final CommandOutcome.EventRecord e = out.event(i);
            if (e.kind() != CommandOutcome.EventKind.REJECT) {
                continue;
            }
            if (takerAsk) {
                risk.release(e.makerUid(), spec.baseCurrency(), e.size() * spec.baseScaleK());
            } else if (fokBudget) {
                risk.release(
                        e.makerUid(), spec.quoteCurrency(), DirectExchangeRisk.bidBudgetHold(spec, e.size(), budget));
            } else {
                risk.release(
                        e.makerUid(),
                        spec.quoteCurrency(),
                        DirectExchangeRisk.bidHold(spec, e.size(), reserveBidPrice));
            }
        }
    }

    // Releases the freed hold for a cancelled or reduced resting order.
    private void releaseReduces(final SymbolSpec spec, final CommandOutcome out) {
        if (spec == null) {
            return;
        }
        final int n = out.eventCount();
        for (int i = 0; i < n; i++) {
            final CommandOutcome.EventRecord e = out.event(i);
            if (e.kind() != CommandOutcome.EventKind.REDUCE) {
                continue;
            }
            if (e.makerBid()) {
                risk.release(
                        e.makerUid(),
                        spec.quoteCurrency(),
                        DirectExchangeRisk.bidHold(spec, e.size(), e.makerReserveBidPrice()));
            } else {
                risk.release(e.makerUid(), spec.baseCurrency(), e.size() * spec.baseScaleK());
            }
        }
    }

    private void handleOrderBookRequest(final CommandEnvelopeDecoder cmd, final CommandOutcome out) {
        final OrderBookNaive book = books.get(cmd.symbolId());
        if (book == null) {
            l2.reset();
        } else {
            book.fillL2(l2);
        }
        out.uid(cmd.uid());
        out.resultCode(CommandResultCode.SUCCESS);
    }

    private OrderBookNaive bookForCreate(final int symbolId) {
        OrderBookNaive book = books.get(symbolId);
        if (book == null) {
            book = new OrderBookNaive(symbolId, orderPool, bucketPool);
            books.put(symbolId, book);
        }
        return book;
    }

    // Clears all engine state; an administrative reset used by tests and benchmarks.
    private void handleReset(final CommandOutcome out) {
        clearState();
        out.resultCode(CommandResultCode.SUCCESS);
    }

    // A no-op acknowledged like any command; useful as a heartbeat or latency probe.
    private void handleNop(final CommandEnvelopeDecoder cmd, final CommandOutcome out) {
        out.uid(cmd.uid());
        out.resultCode(CommandResultCode.SUCCESS);
    }

    // Normalizes an absent (v1) optional fee to zero.
    private static long feeOrZero(final long fee) {
        return fee == CommandEnvelopeDecoder.takerFeeNullValue() ? 0L : fee;
    }

    // Reached only by NULL_VAL / unknown command types.
    private void unsupported(final CommandEnvelopeDecoder cmd, final CommandOutcome out) {
        out.uid(cmd.uid());
        out.resultCode(CommandResultCode.UNSUPPORTED_COMMAND);
        metrics.onUnsupportedCommand();
    }

    /** The reusable L2 snapshot filled by the most recent order-book request. */
    public L2View l2() {
        return l2;
    }

    public OrderBookNaive book(final int symbolId) {
        return books.get(symbolId);
    }

    public int symbolCount() {
        return symbols.size();
    }

    public boolean userExists(final long uid) {
        return accounts.userExists(uid);
    }

    /** Whether the user is suspended (blocked from placing new orders). */
    public boolean isSuspended(final long uid) {
        return accounts.isSuspended(uid);
    }

    public long balance(final long uid, final int currency) {
        return accounts.balance(uid, currency);
    }

    public int userCount() {
        return accounts.userCount();
    }

    public int dedupClientCount() {
        return dedup.clientCount();
    }

    /** Total number of resting orders across every book. */
    public int orderCount() {
        final int[] total = {0};
        books.forEachInt((symbolId, book) -> total[0] += book.orderCount());
        return total[0];
    }

    /** Cumulative cold-path order-node allocations (pool misses); 0 in steady state. */
    public long orderPoolAllocations() {
        return orderPool.allocations();
    }

    /** Cumulative cold-path price-bucket allocations (pool misses); 0 in steady state. */
    public long priceBucketPoolAllocations() {
        return bucketPool.allocations();
    }

    /** Deterministic fingerprint of the full engine state, matching a snapshot checksum. */
    public long stateHash() {
        return SnapshotManager.checksum(symbols, accounts, this::forEachOrderSorted, dedup);
    }

    /** The symbol spec for {@code symbolId}, or {@code null} if unregistered. */
    public SymbolSpec symbolSpec(final int symbolId) {
        return symbols.get(symbolId);
    }

    /** Emits every resting order across all books in deterministic order; cold read path. */
    public void forEachOrder(final SnapshotManager.OrderSink sink) {
        forEachOrderSorted(sink);
    }

    /** Emits every balance entry in ascending {@code (uid, currency)} order; cold read path. */
    public void forEachBalance(final AccountStore.BalanceConsumer consumer) {
        accounts.forEachSorted(consumer);
    }

    /** Writes the full engine state to a snapshot sink in deterministic order. */
    public void writeSnapshot(
            final SnapshotManager snapshotManager,
            final SnapshotManager.SnapshotSink sink,
            final Runnable idler,
            final long logPosition) {
        snapshotManager.write(sink, idler, symbols, accounts, this::forEachOrderSorted, dedup, logPosition);
    }

    /** Clears this engine's state and prepares its stores to receive a snapshot load. */
    public void beginSnapshotLoad(final SnapshotManager snapshotManager) {
        books.clear();
        snapshotManager.beginLoad(symbols, accounts, this::forEachOrderSorted, this::restoreOrder, dedup);
    }

    /** Clears all engine state; used to discard a rejected snapshot load. */
    public void clearState() {
        symbols.clear();
        accounts.clear();
        dedup.clear();
        books.clear();
    }

    // Emits every resting order in ascending symbol order, each book best-first FIFO.
    private void forEachOrderSorted(final SnapshotManager.OrderSink sink) {
        final int count = books.size();
        if (symbolScratch.length < count) {
            symbolScratch = new int[count];
        }
        final int[] ids = symbolScratch;
        final int[] cursor = {0};
        books.forEachInt((symbolId, book) -> ids[cursor[0]++] = symbolId);
        Arrays.sort(ids, 0, count);
        for (int i = 0; i < count; i++) {
            final int symbolId = ids[i];
            final OrderBookNaive book = books.get(symbolId);
            book.forEachOrderSorted((orderId, ask, price, size, filled, reserveBidPrice, uid, timestamp) ->
                    sink.accept(symbolId, orderId, ask, price, size, filled, reserveBidPrice, uid, timestamp));
        }
    }

    // Reinstates one resting order into its book (creating the book if needed).
    private void restoreOrder(
            final int symbolId,
            final long orderId,
            final boolean ask,
            final long price,
            final long size,
            final long filled,
            final long reserveBidPrice,
            final long uid,
            final long timestamp) {
        bookForCreate(symbolId).restingInsert(orderId, ask, price, size, filled, reserveBidPrice, uid, timestamp);
    }
}
