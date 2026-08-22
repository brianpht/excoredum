package com.exadbe.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.core.CommandOutcome;
import com.exadbe.protocol.CommandEnvelopeDecoder;
import com.exadbe.protocol.CommandEnvelopeEncoder;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.protocol.MessageHeaderDecoder;
import com.exadbe.protocol.MessageHeaderEncoder;
import com.exadbe.protocol.OrderAction;
import com.exadbe.protocol.OrderCommandType;
import com.exadbe.protocol.OrderType;
import com.exadbe.read.order.MarketTrade;
import com.exadbe.read.order.OrderLedger;
import com.exadbe.read.order.OrderRecord;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OrderLedger}: lifecycle state transitions, fills, dedup
 * skip, eviction, userCookie passthrough, and the market trade tape, driven by
 * hand-built command envelopes and outcomes (no engine, no network).
 */
final class OrderLedgerTest {

    private static final int SYMBOL = 1;
    private static final long MAKER = 11L;
    private static final long TAKER = 22L;

    private final OrderLedger ledger = new OrderLedger();
    private final CommandOutcome outcome = new CommandOutcome(32);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final CommandEnvelopeEncoder envelopeEncoder = new CommandEnvelopeEncoder();
    private final CommandEnvelopeDecoder envelopeDecoder = new CommandEnvelopeDecoder();
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[512]);

    @Test
    void restingPlaceIsActiveWithPlacementFields() {
        applyPlace(1L, 100L, MAKER, true, OrderType.GTC, 100L, 10L, 7, CommandResultCode.SUCCESS, 0L);

        final List<OrderRecord> history = ledger.orderHistory(MAKER);
        assertEquals(1, history.size());
        final OrderRecord record = history.get(0);
        assertEquals(100L, record.orderId());
        assertEquals(MAKER, record.uid());
        assertEquals(SYMBOL, record.symbolId());
        assertTrue(record.ask());
        assertEquals("GTC", record.orderType());
        assertEquals(7, record.userCookie());
        assertEquals(100L, record.price());
        assertEquals(10L, record.size());
        assertEquals(0L, record.filled());
        assertEquals(0L, record.reduced());
        assertEquals(10L, record.remaining());
        assertEquals(OrderRecord.STATE_ACTIVE, record.state());
        assertEquals("ACTIVE", record.stateName());
        assertTrue(record.placedTimestamp() > 0L);
        assertEquals(record.placedTimestamp(), record.lastTimestamp());
        assertTrue(record.fills().isEmpty());
        assertEquals(record, ledger.order(100L));
        assertEquals(1, ledger.activeOrders(MAKER).size());
    }

    @Test
    void takerFillsRecordDealsAndCompleteMakerOnFullConsumption() {
        applyPlace(1L, 1L, MAKER, true, OrderType.GTC, 100L, 10L, 0, CommandResultCode.SUCCESS, 0L);
        // Taker bid 6 @ 105 crosses the resting ask; the maker keeps 4 resting.
        outcome.reset(0L, 2L);
        outcome.uid(TAKER);
        outcome.orderId(2L);
        outcome.filledSize(6L);
        outcome.resultCode(CommandResultCode.SUCCESS);
        outcome.addTrade(SYMBOL, 1L, MAKER, TAKER, 100L, 6L, false, false, 0L, true, 630L);
        ledger.applyCommand(1000L, place(2L, 2L, TAKER, false, OrderType.GTC, 105L, 6L, 0), outcome);

        final OrderRecord maker = ledger.order(1L);
        assertEquals(OrderRecord.STATE_ACTIVE, maker.state(), "a partially filled maker keeps resting");
        assertEquals(6L, maker.filled());
        assertEquals(4L, maker.remaining());
        assertEquals(1, maker.fills().size());
        assertFalse(maker.fills().get(0).taker());
        assertEquals(TAKER, maker.fills().get(0).counterpartyUid());

        final OrderRecord taker = ledger.order(2L);
        assertEquals(OrderRecord.STATE_COMPLETED, taker.state());
        assertEquals(6L, taker.filled());
        assertEquals(1, taker.fills().size());
        assertTrue(taker.fills().get(0).taker());
        assertEquals(MAKER, taker.fills().get(0).counterpartyUid());

        // A second taker consumes the maker's remainder: the maker completes.
        outcome.reset(0L, 3L);
        outcome.uid(TAKER);
        outcome.orderId(3L);
        outcome.filledSize(4L);
        outcome.resultCode(CommandResultCode.SUCCESS);
        outcome.addTrade(SYMBOL, 1L, MAKER, TAKER, 100L, 4L, true, false, 0L, true, 420L);
        ledger.applyCommand(1100L, place(3L, 3L, TAKER, false, OrderType.GTC, 105L, 4L, 0), outcome);

        final OrderRecord consumedMaker = ledger.order(1L);
        assertEquals(OrderRecord.STATE_COMPLETED, consumedMaker.state(), "full consumption completes the maker");
        assertEquals(10L, consumedMaker.filled());
        assertEquals(0L, consumedMaker.remaining());
        assertEquals(2, consumedMaker.fills().size());

        final List<MarketTrade> trades = ledger.marketTrades(SYMBOL, 10);
        assertEquals(2, trades.size(), "one trade per taker command");
        assertEquals(4L, trades.get(0).size(), "newest trade first");
        assertEquals(1100L, trades.get(0).timestamp());
        assertEquals(6L, trades.get(1).size());
        assertEquals(1L, trades.get(1).makerOrderId());
        assertEquals(2, ledger.userTrades(MAKER, 10).size());
        assertEquals(2, ledger.userTrades(TAKER, 10).size());
        assertEquals(0, ledger.userTrades(99L, 10).size());
        assertEquals(0, ledger.marketTrades(99, 10).size());
        assertTrue(ledger.activeOrders(MAKER).isEmpty());
    }

    @Test
    void cancelEmitsReduceAndMarksCancelled() {
        applyPlace(1L, 100L, MAKER, true, OrderType.GTC, 100L, 10L, 0, CommandResultCode.SUCCESS, 0L);
        outcome.reset(0L, 2L);
        outcome.uid(MAKER);
        outcome.orderId(100L);
        outcome.resultCode(CommandResultCode.SUCCESS);
        outcome.addReduce(SYMBOL, 100L, MAKER, 10L, true, 0L, 100L, true);
        ledger.applyCommand(2000L, cancel(2L, 100L, MAKER), outcome);

        final OrderRecord record = ledger.order(100L);
        assertEquals(OrderRecord.STATE_CANCELLED, record.state());
        assertEquals(10L, record.reduced());
        assertEquals(0L, record.remaining());
        assertEquals(2000L, record.lastTimestamp());
        assertTrue(ledger.activeOrders(MAKER).isEmpty());
    }

    @Test
    void partialReduceShrinksRemainingOnly() {
        applyPlace(1L, 100L, MAKER, true, OrderType.GTC, 100L, 10L, 0, CommandResultCode.SUCCESS, 0L);
        outcome.reset(0L, 2L);
        outcome.uid(MAKER);
        outcome.orderId(100L);
        outcome.resultCode(CommandResultCode.SUCCESS);
        outcome.addReduce(SYMBOL, 100L, MAKER, 3L, true, 0L, 100L, false);
        ledger.applyCommand(2000L, reduce(2L, 100L, MAKER, 3L), outcome);

        final OrderRecord record = ledger.order(100L);
        assertEquals(OrderRecord.STATE_ACTIVE, record.state());
        assertEquals(3L, record.reduced());
        assertEquals(7L, record.remaining());
    }

    @Test
    void iocPartialFillThenReject() {
        applyPlace(1L, 1L, TAKER, false, OrderType.GTC, 95L, 4L, 0, CommandResultCode.SUCCESS, 0L);
        // IOC ask 10 @ 95 fills 4 against the resting bid, rejects the remainder.
        outcome.reset(0L, 2L);
        outcome.uid(TAKER);
        outcome.orderId(2L);
        outcome.filledSize(4L);
        outcome.resultCode(CommandResultCode.SUCCESS);
        outcome.addTrade(SYMBOL, 1L, TAKER, TAKER, 95L, 4L, true, true, 0L, false, 0L);
        outcome.addReject(SYMBOL, 2L, TAKER, 6L, 95L);
        ledger.applyCommand(1000L, place(2L, 2L, TAKER, true, OrderType.IOC, 95L, 10L, 0), outcome);

        final OrderRecord record = ledger.order(2L);
        assertEquals(OrderRecord.STATE_REJECTED, record.state());
        assertEquals(4L, record.filled());
        assertEquals(10L, record.size());
        assertEquals(6L, record.remaining());
        assertEquals("IOC", record.orderType());
    }

    @Test
    void rejectedPlaceIsKeptAsRejected() {
        applyPlace(1L, 100L, MAKER, true, OrderType.GTC, 100L, 10L, 0, CommandResultCode.USER_NOT_FOUND, 0L);

        final OrderRecord record = ledger.order(100L);
        assertEquals(OrderRecord.STATE_REJECTED, record.state());
        assertEquals(1, ledger.orderHistory(MAKER).size());
        assertTrue(ledger.activeOrders(MAKER).isEmpty());
    }

    @Test
    void duplicateCommandIsSkipped() {
        applyPlace(1L, 100L, MAKER, true, OrderType.GTC, 100L, 10L, 0, CommandResultCode.SUCCESS, 0L);
        // Same placement re-delivered: the ordersById guard (plus a zero-event
        // cached outcome) prevents a second record without re-applying fills.
        applyPlace(2L, 100L, MAKER, true, OrderType.GTC, 100L, 10L, 0, CommandResultCode.DUPLICATE, 0L);

        assertEquals(1, ledger.orderHistory(MAKER).size());
        assertEquals(0L, ledger.order(100L).filled(), "a re-delivered command must not re-apply");
    }

    @Test
    void moveUpdatesPrice() {
        applyPlace(1L, 100L, MAKER, true, OrderType.GTC, 100L, 10L, 0, CommandResultCode.SUCCESS, 0L);
        outcome.reset(0L, 2L);
        outcome.uid(MAKER);
        outcome.orderId(100L);
        outcome.resultCode(CommandResultCode.SUCCESS);
        ledger.applyCommand(2000L, move(2L, 100L, MAKER, 110L), outcome);

        assertEquals(110L, ledger.order(100L).price());
        assertEquals(2000L, ledger.order(100L).lastTimestamp());
    }

    @Test
    void userCookieNormalizesAbsentToZero() {
        applyPlace(
                1L,
                1L,
                MAKER,
                true,
                OrderType.GTC,
                100L,
                10L,
                CommandEnvelopeEncoder.userCookieNullValue(),
                CommandResultCode.SUCCESS,
                0L);
        applyPlace(2L, 2L, MAKER, true, OrderType.GTC, 100L, 10L, 42, CommandResultCode.SUCCESS, 0L);

        assertEquals(0, ledger.order(1L).userCookie());
        assertEquals(42, ledger.order(2L).userCookie());
    }

    @Test
    void fokBudgetStoresBudgetAsPrice() {
        applyPlace(1L, 100L, TAKER, false, OrderType.FOK_BUDGET, 600L, 6L, 0, CommandResultCode.SUCCESS, 6L);

        final OrderRecord record = ledger.order(100L);
        assertEquals("FOK_BUDGET", record.orderType());
        assertEquals(600L, record.price());
        assertEquals(OrderRecord.STATE_COMPLETED, record.state());
    }

    @Test
    void evictionDropsOldestTerminalRecordsPerUser() {
        final int capacity = OrderLedger.MAX_ORDERS_PER_USER;
        // Fill the user up with resting (active) orders, then terminate the oldest half.
        for (int i = 0; i < capacity; i++) {
            applyPlace(
                    (long) i + 1, (long) i + 1, MAKER, true, OrderType.GTC, 100L, 1L, 0, CommandResultCode.SUCCESS, 0L);
        }
        for (int i = 0; i < capacity / 2; i++) {
            outcome.reset(0L, (long) i + 1);
            outcome.uid(MAKER);
            outcome.orderId((long) i + 1);
            outcome.resultCode(CommandResultCode.SUCCESS);
            outcome.addReduce(SYMBOL, (long) i + 1, MAKER, 1L, true, 0L, 100L, true);
            ledger.applyCommand(1000L, cancel(200L + i, (long) i + 1, MAKER), outcome);
        }
        // Place one more order to trigger eviction of the oldest terminal record.
        applyPlace(9999L, 9999L, MAKER, true, OrderType.GTC, 100L, 1L, 0, CommandResultCode.SUCCESS, 0L);

        final List<OrderRecord> history = ledger.orderHistory(MAKER);
        assertEquals(capacity, history.size(), "over-capacity placement evicts exactly one terminal record");
        assertNull(ledger.order(1L), "the oldest cancelled order must have been evicted");
        assertTrue(ledger.order(capacity / 2L) != null, "younger terminal records stay");
    }

    @Test
    void resetClearsLedger() {
        applyPlace(1L, 100L, MAKER, true, OrderType.GTC, 100L, 10L, 0, CommandResultCode.SUCCESS, 0L);
        outcome.reset(0L, 2L);
        outcome.resultCode(CommandResultCode.SUCCESS);
        ledger.applyCommand(2000L, resetCmd(2L), outcome);

        assertTrue(ledger.orderHistory(MAKER).isEmpty());
        assertNull(ledger.order(100L));
        assertTrue(ledger.marketTrades(SYMBOL, 10).isEmpty());
    }

    private void applyPlace(
            final long clientSeq,
            final long orderId,
            final long uid,
            final boolean ask,
            final OrderType orderType,
            final long price,
            final long size,
            final int userCookie,
            final CommandResultCode resultCode,
            final long filledSize) {
        outcome.reset(0L, clientSeq);
        outcome.uid(uid);
        outcome.orderId(orderId);
        if (resultCode == CommandResultCode.SUCCESS && filledSize >= 0L) {
            outcome.filledSize(filledSize);
        }
        outcome.resultCode(resultCode);
        ledger.applyCommand(
                clientSeq * 1000L, place(clientSeq, orderId, uid, ask, orderType, price, size, userCookie), outcome);
    }

    private CommandEnvelopeDecoder place(
            final long clientSeq,
            final long orderId,
            final long uid,
            final boolean ask,
            final OrderType orderType,
            final long price,
            final long size,
            final int userCookie) {
        return encode(
                clientSeq,
                OrderCommandType.PLACE_ORDER,
                uid,
                orderId,
                ask ? OrderAction.ASK : OrderAction.BID,
                orderType,
                price,
                size,
                userCookie);
    }

    private CommandEnvelopeDecoder move(final long clientSeq, final long orderId, final long uid, final long price) {
        return encode(
                clientSeq,
                OrderCommandType.MOVE_ORDER,
                uid,
                orderId,
                OrderAction.NULL_VAL,
                OrderType.NULL_VAL,
                price,
                CommandEnvelopeEncoder.sizeNullValue(),
                CommandEnvelopeEncoder.userCookieNullValue());
    }

    private CommandEnvelopeDecoder cancel(final long clientSeq, final long orderId, final long uid) {
        return encode(
                clientSeq,
                OrderCommandType.CANCEL_ORDER,
                uid,
                orderId,
                OrderAction.NULL_VAL,
                OrderType.NULL_VAL,
                CommandEnvelopeEncoder.priceNullValue(),
                CommandEnvelopeEncoder.sizeNullValue(),
                CommandEnvelopeEncoder.userCookieNullValue());
    }

    private CommandEnvelopeDecoder reduce(final long clientSeq, final long orderId, final long uid, final long size) {
        return encode(
                clientSeq,
                OrderCommandType.REDUCE_ORDER,
                uid,
                orderId,
                OrderAction.NULL_VAL,
                OrderType.NULL_VAL,
                CommandEnvelopeEncoder.priceNullValue(),
                size,
                CommandEnvelopeEncoder.userCookieNullValue());
    }

    private CommandEnvelopeDecoder resetCmd(final long clientSeq) {
        return encode(
                clientSeq,
                OrderCommandType.RESET,
                0L,
                CommandEnvelopeEncoder.orderIdNullValue(),
                OrderAction.NULL_VAL,
                OrderType.NULL_VAL,
                CommandEnvelopeEncoder.priceNullValue(),
                CommandEnvelopeEncoder.sizeNullValue(),
                CommandEnvelopeEncoder.userCookieNullValue());
    }

    private CommandEnvelopeDecoder encode(
            final long clientSeq,
            final OrderCommandType type,
            final long uid,
            final long orderId,
            final OrderAction action,
            final OrderType orderType,
            final long price,
            final long size,
            final int userCookie) {
        envelopeEncoder
                .wrapAndApplyHeader(buffer, 0, headerEncoder)
                .clientId(1L)
                .clientSeq(clientSeq)
                .commandIdHi(1L)
                .commandIdLo(clientSeq)
                .commandType(type)
                .uid(uid)
                .symbolId(SYMBOL)
                .orderId(orderId)
                .price(price)
                .reserveBidPrice(CommandEnvelopeEncoder.reserveBidPriceNullValue())
                .size(size)
                .action(action)
                .orderType(orderType)
                .userCookie(userCookie);
        headerDecoder.wrap(buffer, 0);
        envelopeDecoder.wrap(
                buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
        return envelopeDecoder;
    }
}
