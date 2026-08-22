package com.exadbe.read.order;

import com.exadbe.core.CommandOutcome;
import com.exadbe.protocol.CommandEnvelopeDecoder;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.protocol.OrderAction;
import com.exadbe.protocol.OrderCommandType;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * The read side's order lifecycle ledger: per-user order history, per-order
 * fills, and a bounded market trade tape, rebuilt deterministically from the
 * replicated command log. Owned by the read replica's single polling thread;
 * it is updated as each command is applied and queried from the same thread.
 *
 * <p>Re-delivered commands are already deduplicated upstream: the engine replays
 * the cached outcome verbatim, which carries zero matcher events, so
 * {@link #applyEvents} has nothing to double-count, and {@link #applyPlace}
 * guards on {@code ordersById} so a re-applied place cannot add a second record.
 * A replicated RESET clears the ledger, mirroring the engine's state reset.
 *
 * <p>Memory is bounded: each user keeps at most {@link #MAX_ORDERS_PER_USER}
 * records (oldest terminal records are evicted; resting orders are already
 * bounded by the engine's order pool) and the trade tape holds at most
 * {@link #MAX_MARKET_TRADES} entries, overwriting the oldest.
 */
public final class OrderLedger {

    /** Per-user cap on retained order records; oldest terminal records are evicted first. */
    public static final int MAX_ORDERS_PER_USER = 4096;

    /** Cap on the global market trade tape; oldest trades are overwritten. */
    public static final int MAX_MARKET_TRADES = 65536;

    private static final float LOAD_FACTOR = 0.65f;

    private static final class UserHistory {
        final Long2ObjectHashMap<OrderRecord> byId = new Long2ObjectHashMap<>(16, LOAD_FACTOR);
        final ArrayList<OrderRecord> placementOrder = new ArrayList<>(16);
    }

    private final Long2ObjectHashMap<UserHistory> users = new Long2ObjectHashMap<>(64, LOAD_FACTOR);
    private final Long2ObjectHashMap<OrderRecord> ordersById = new Long2ObjectHashMap<>(1024, LOAD_FACTOR);
    private final MarketTrade[] tape = new MarketTrade[MAX_MARKET_TRADES];
    private int tapeHead;
    private int tapeCount;

    /**
     * Applies one applied command to the ledger. Re-delivered commands are
     * deduplicated upstream (their cached outcome carries zero events), so no
     * explicit duplicate guard is needed here.
     */
    public void applyCommand(
            final long timestamp, final CommandEnvelopeDecoder envelope, final CommandOutcome outcome) {
        switch (envelope.commandType()) {
            case PLACE_ORDER -> applyPlace(timestamp, envelope, outcome);
            case MOVE_ORDER -> applyMove(timestamp, envelope, outcome);
            case RESET -> clear();
            default -> {
                // No ledger effect for user, balance, symbol, book-request, or no-op commands.
            }
        }
        applyEvents(timestamp, envelope, outcome);
    }

    /** Drops all ledger state; mirrors a replicated RESET command. */
    public void clear() {
        users.clear();
        ordersById.clear();
        tapeHead = 0;
        tapeCount = 0;
    }

    /**
     * Serializes the ledger to {@code out} for a local checkpoint: every user's
     * records in placement order (users sorted by uid for determinism) followed
     * by the market trade tape oldest-first.
     */
    public void writeTo(final DataOutput out) throws IOException {
        final long[] uids = new long[users.size()];
        final int[] cursor = {0};
        users.forEachLong((uid, user) -> uids[cursor[0]++] = uid);
        Arrays.sort(uids, 0, users.size());

        out.writeInt(users.size());
        for (int u = 0; u < users.size(); u++) {
            final UserHistory user = users.get(uids[u]);
            out.writeLong(uids[u]);
            out.writeInt(user.placementOrder.size());
            for (final OrderRecord record : user.placementOrder) {
                record.writeTo(out);
            }
        }
        out.writeInt(tapeCount);
        for (int i = 0; i < tapeCount; i++) {
            final MarketTrade trade = tape[(tapeHead + i) % MAX_MARKET_TRADES];
            out.writeLong(trade.timestamp());
            out.writeInt(trade.symbolId());
            out.writeLong(trade.price());
            out.writeLong(trade.size());
            out.writeLong(trade.makerOrderId());
            out.writeLong(trade.makerUid());
            out.writeLong(trade.takerUid());
        }
    }

    /** Restores a ledger previously written by {@link #writeTo}. */
    public void readFrom(final DataInput in) throws IOException {
        clear();
        final int userCount = in.readInt();
        for (int u = 0; u < userCount; u++) {
            final long uid = in.readLong();
            final UserHistory user = users.computeIfAbsent(uid, ignored -> new UserHistory());
            final int recordCount = in.readInt();
            for (int r = 0; r < recordCount; r++) {
                final OrderRecord record = OrderRecord.readFrom(in);
                user.byId.put(record.orderId(), record);
                user.placementOrder.add(record);
                ordersById.put(record.orderId(), record);
            }
        }
        final int restoredTapeCount = in.readInt();
        tapeHead = 0;
        tapeCount = restoredTapeCount;
        for (int i = 0; i < restoredTapeCount; i++) {
            tape[i] = new MarketTrade(
                    in.readLong(),
                    in.readInt(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong());
        }
    }

    /** Every tracked order of {@code uid} in placement order, oldest first. */
    public List<OrderRecord> orderHistory(final long uid) {
        final UserHistory user = users.get(uid);
        return user == null ? List.of() : List.copyOf(user.placementOrder);
    }

    /** The still-resting orders of {@code uid} in placement order. */
    public List<OrderRecord> activeOrders(final long uid) {
        final UserHistory user = users.get(uid);
        if (user == null) {
            return List.of();
        }
        final ArrayList<OrderRecord> active = new ArrayList<>(user.placementOrder.size());
        for (final OrderRecord record : user.placementOrder) {
            if (record.state() == OrderRecord.STATE_ACTIVE) {
                active.add(record);
            }
        }
        return active;
    }

    /** The tracked record for {@code orderId}, or {@code null} when unknown. */
    public OrderRecord order(final long orderId) {
        return ordersById.get(orderId);
    }

    /** The most recent {@code limit} trades involving {@code uid} as maker or taker. */
    public List<MarketTrade> userTrades(final long uid, final int limit) {
        final ArrayList<MarketTrade> trades = new ArrayList<>(Math.min(limit, tapeCount));
        forEachNewestFirst(trade -> {
            if (trades.size() < limit && (trade.makerUid() == uid || trade.takerUid() == uid)) {
                trades.add(trade);
            }
        });
        return trades;
    }

    /** The most recent {@code limit} trades of {@code symbolId}. */
    public List<MarketTrade> marketTrades(final int symbolId, final int limit) {
        final ArrayList<MarketTrade> trades = new ArrayList<>(Math.min(limit, tapeCount));
        forEachNewestFirst(trade -> {
            if (trades.size() < limit && trade.symbolId() == symbolId) {
                trades.add(trade);
            }
        });
        return trades;
    }

    private void applyPlace(final long timestamp, final CommandEnvelopeDecoder envelope, final CommandOutcome outcome) {
        final long orderId = envelope.orderId();
        if (ordersById.containsKey(orderId)) {
            return;
        }
        final long uid = envelope.uid();
        final UserHistory user = users.computeIfAbsent(uid, ignored -> new UserHistory());
        final OrderRecord record = new OrderRecord(
                orderId,
                uid,
                envelope.symbolId(),
                envelope.action() == OrderAction.ASK,
                envelope.orderType().name(),
                normalizedUserCookie(envelope.userCookie()),
                envelope.price(),
                envelope.size(),
                timestamp);
        user.byId.put(orderId, record);
        user.placementOrder.add(record);
        ordersById.put(orderId, record);
        if (outcome.resultCode() == CommandResultCode.SUCCESS) {
            record.state(
                    outcome.hasFilledSize() && outcome.filledSize() >= record.size()
                            ? OrderRecord.STATE_COMPLETED
                            : OrderRecord.STATE_ACTIVE);
        } else {
            record.state(OrderRecord.STATE_REJECTED);
        }
        evictTerminal(user);
    }

    private void applyMove(final long timestamp, final CommandEnvelopeDecoder envelope, final CommandOutcome outcome) {
        if (outcome.resultCode() != CommandResultCode.SUCCESS) {
            return;
        }
        final OrderRecord record = ordersById.get(envelope.orderId());
        if (record != null) {
            record.price(envelope.price());
            record.touch(timestamp);
        }
    }

    private void applyEvents(
            final long timestamp, final CommandEnvelopeDecoder envelope, final CommandOutcome outcome) {
        final OrderCommandType type = envelope.commandType();
        final boolean aggressor = type == OrderCommandType.PLACE_ORDER || type == OrderCommandType.MOVE_ORDER;
        final long takerOrderId = envelope.orderId();
        final int n = outcome.eventCount();
        for (int i = 0; i < n; i++) {
            final CommandOutcome.EventRecord event = outcome.event(i);
            switch (event.kind()) {
                case TRADE -> {
                    addTrade(
                            timestamp,
                            event.symbolId(),
                            event.price(),
                            event.size(),
                            event.makerOrderId(),
                            event.makerUid(),
                            event.takerUid());
                    final OrderRecord maker = ordersById.get(event.makerOrderId());
                    if (maker != null) {
                        maker.addFill(false, event.price(), event.size(), event.takerUid(), timestamp);
                        if (event.makerCompleted()) {
                            maker.state(OrderRecord.STATE_COMPLETED);
                        }
                    }
                    if (aggressor) {
                        final OrderRecord taker = ordersById.get(takerOrderId);
                        if (taker != null) {
                            taker.addFill(true, event.price(), event.size(), event.makerUid(), timestamp);
                        }
                    }
                }
                case REDUCE -> {
                    final OrderRecord record = ordersById.get(event.makerOrderId());
                    if (record != null) {
                        record.addReduce(event.size(), timestamp);
                        if (event.makerCompleted() && record.state() != OrderRecord.STATE_COMPLETED) {
                            record.state(OrderRecord.STATE_CANCELLED);
                        }
                    }
                }
                case REJECT -> {
                    final OrderRecord record = ordersById.get(event.makerOrderId());
                    if (record != null) {
                        record.state(OrderRecord.STATE_REJECTED);
                    }
                }
                default -> {
                    // Unknown event kinds are ignored; the protocol only emits the three above.
                }
            }
        }
    }

    private void addTrade(
            final long timestamp,
            final int symbolId,
            final long price,
            final long size,
            final long makerOrderId,
            final long makerUid,
            final long takerUid) {
        if (tapeCount < MAX_MARKET_TRADES) {
            tape[(tapeHead + tapeCount) % MAX_MARKET_TRADES] =
                    new MarketTrade(timestamp, symbolId, price, size, makerOrderId, makerUid, takerUid);
            tapeCount++;
        } else {
            tape[tapeHead] = new MarketTrade(timestamp, symbolId, price, size, makerOrderId, makerUid, takerUid);
            tapeHead = (tapeHead + 1) % MAX_MARKET_TRADES;
        }
    }

    private void forEachNewestFirst(final java.util.function.Consumer<MarketTrade> visitor) {
        for (int i = 0; i < tapeCount; i++) {
            final int index = (tapeHead + tapeCount - 1 - i) % MAX_MARKET_TRADES;
            visitor.accept(tape[index]);
        }
    }

    private void evictTerminal(final UserHistory user) {
        while (user.placementOrder.size() > MAX_ORDERS_PER_USER) {
            boolean evicted = false;
            for (int i = 0; i < user.placementOrder.size(); i++) {
                final OrderRecord record = user.placementOrder.get(i);
                if (record.state() != OrderRecord.STATE_ACTIVE && record.state() != OrderRecord.STATE_NEW) {
                    user.byId.remove(record.orderId());
                    ordersById.remove(record.orderId());
                    user.placementOrder.remove(i);
                    evicted = true;
                    break;
                }
            }
            if (!evicted) {
                // All records are still resting; resting orders are bounded by the engine's order pool.
                return;
            }
        }
    }

    private static int normalizedUserCookie(final int userCookie) {
        return userCookie == CommandEnvelopeDecoder.userCookieNullValue() ? 0 : userCookie;
    }
}
