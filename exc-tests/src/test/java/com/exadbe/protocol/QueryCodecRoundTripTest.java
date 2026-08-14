package com.exadbe.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/** Unit: the query protocol codecs round-trip every group and scalar intact. */
class QueryCodecRoundTripTest {

    @Test
    void roundTripsFullQueryResponse() {
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final QueryResponseEncoder encoder = new QueryResponseEncoder();
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);

        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .requestId(42L)
                .queryType(QueryType.ORDER_HISTORY)
                .status(QueryStatusCode.SUCCESS)
                .appliedPosition(123_456L)
                .uid(7L);

        final QueryResponseEncoder.AsksEncoder asks = encoder.asksCount(1);
        asks.next().price(101L).size(4L).orders(1);
        encoder.bidsCount(0);

        final QueryResponseEncoder.HistoryEncoder history = encoder.historyCount(1);
        final QueryResponseEncoder.HistoryEncoder record = history.next();
        record.symbolId(1)
                .orderId(9L)
                .uid(7L)
                .ask((short) 1)
                .orderType(OrderType.GTC)
                .price(101L)
                .size(10L)
                .filled(6L)
                .reduced(0L)
                .lastTimestamp(1_000L)
                .placedTimestamp(900L)
                .userCookie(111)
                .state((short) 1);
        final QueryResponseEncoder.HistoryEncoder.FillsEncoder fills = record.fillsCount(1);
        fills.next().price(100L).size(6L).taker((short) 0).counterpartyUid(8L).timestamp(950L);

        final QueryResponseEncoder.TradesEncoder trades = encoder.tradesCount(1);
        trades.next()
                .symbolId(1)
                .makerOrderId(9L)
                .makerUid(7L)
                .takerUid(8L)
                .price(100L)
                .size(6L)
                .timestamp(950L);

        final QueryResponseEncoder.TotalsEncoder totals = encoder.totalsCount(1);
        totals.next().currency(10).accountBalance(996L).reserved(4L).fees(0L);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
        assertTrue(length > MessageHeaderEncoder.ENCODED_LENGTH);

        final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        final QueryResponseDecoder decoder = new QueryResponseDecoder();
        headerDecoder.wrap(buffer, 0);
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(42L, decoder.requestId());
        assertEquals(QueryType.ORDER_HISTORY, decoder.queryType());
        assertEquals(QueryStatusCode.SUCCESS, decoder.status());
        assertEquals(123_456L, decoder.appliedPosition());
        assertEquals(7L, decoder.uid());

        final QueryResponseDecoder.AsksDecoder asksIt = decoder.asks();
        assertTrue(asksIt.hasNext());
        final QueryResponseDecoder.AsksDecoder ask = asksIt.next();
        assertEquals(101L, ask.price());
        assertEquals(4L, ask.size());
        assertEquals(1, ask.orders());
        assertFalse(asksIt.hasNext());
        assertEquals(0, decoder.bids().count());

        final QueryResponseDecoder.HistoryDecoder historyIt = decoder.history();
        assertTrue(historyIt.hasNext());
        final QueryResponseDecoder.HistoryDecoder rec = historyIt.next();
        assertEquals(1, rec.symbolId());
        assertEquals(9L, rec.orderId());
        assertEquals(7L, rec.uid());
        assertEquals((short) 1, rec.ask());
        assertEquals(OrderType.GTC, rec.orderType());
        assertEquals(101L, rec.price());
        assertEquals(10L, rec.size());
        assertEquals(6L, rec.filled());
        assertEquals(0L, rec.reduced());
        assertEquals(1_000L, rec.lastTimestamp());
        assertEquals(900L, rec.placedTimestamp());
        assertEquals(111, rec.userCookie());
        assertEquals((short) 1, rec.state());
        final QueryResponseDecoder.HistoryDecoder.FillsDecoder fillsIt = rec.fills();
        assertTrue(fillsIt.hasNext());
        final QueryResponseDecoder.HistoryDecoder.FillsDecoder fill = fillsIt.next();
        assertEquals(100L, fill.price());
        assertEquals(6L, fill.size());
        assertEquals((short) 0, fill.taker());
        assertEquals(8L, fill.counterpartyUid());
        assertEquals(950L, fill.timestamp());
        assertFalse(fillsIt.hasNext());
        assertFalse(historyIt.hasNext());

        final QueryResponseDecoder.TradesDecoder tradesIt = decoder.trades();
        assertTrue(tradesIt.hasNext());
        final QueryResponseDecoder.TradesDecoder trade = tradesIt.next();
        assertEquals(1, trade.symbolId());
        assertEquals(9L, trade.makerOrderId());
        assertEquals(7L, trade.makerUid());
        assertEquals(8L, trade.takerUid());
        assertEquals(100L, trade.price());
        assertEquals(6L, trade.size());
        assertEquals(950L, trade.timestamp());

        final QueryResponseDecoder.TotalsDecoder totalsIt = decoder.totals();
        assertTrue(totalsIt.hasNext());
        final QueryResponseDecoder.TotalsDecoder total = totalsIt.next();
        assertEquals(10, total.currency());
        assertEquals(996L, total.accountBalance());
        assertEquals(4L, total.reserved());
        assertEquals(0L, total.fees());
    }

    @Test
    void roundTripsQueryRequestWithResponseChannel() {
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final QueryRequestEncoder encoder = new QueryRequestEncoder();
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[512]);

        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .requestId(7L)
                .queryType(QueryType.L2_ORDER_BOOK)
                .uid(QueryRequestEncoder.uidNullValue())
                .currency(QueryRequestEncoder.currencyNullValue())
                .symbolId(3)
                .orderId(QueryRequestEncoder.orderIdNullValue())
                .tradeLimit(QueryRequestEncoder.tradeLimitNullValue())
                .maxLevels(10)
                .responseStreamId(301)
                .responseChannel("aeron:udp?endpoint=127.0.0.1:54321");

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();

        final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        final QueryRequestDecoder decoder = new QueryRequestDecoder();
        headerDecoder.wrap(buffer, 0);
        decoder.wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(7L, decoder.requestId());
        assertEquals(QueryType.L2_ORDER_BOOK, decoder.queryType());
        assertEquals(3, decoder.symbolId());
        assertEquals(10, decoder.maxLevels());
        assertEquals(301L, decoder.responseStreamId());
        assertEquals("aeron:udp?endpoint=127.0.0.1:54321", decoder.responseChannel());
        assertEquals(QueryRequestDecoder.uidNullValue(), decoder.uid());
    }
}
