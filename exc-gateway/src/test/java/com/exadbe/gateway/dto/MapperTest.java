package com.exadbe.gateway.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.read.client.L2Snapshot;
import com.exadbe.read.client.MarketTradeResult;
import com.exadbe.read.client.OrderRecordResult;
import com.exadbe.read.client.OrderState;
import com.exadbe.read.client.TotalBalanceResult;
import com.exadbe.read.client.UserReport;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Verifies the gateway {@link Mapper} turns read-side records into fixed DTO shapes. */
class MapperTest {

    @Test
    void mapsOrderBook() {
        final L2Snapshot snapshot = new L2Snapshot(
                1, true, 42, List.of(new L2Snapshot.Level(101L, 4L, 1)), List.of(new L2Snapshot.Level(99L, 2L, 3)));
        final OrderBookDto dto = Mapper.orderBook(snapshot);
        assertEquals(1, dto.symbolId());
        assertTrue(dto.found());
        assertEquals(42, dto.appliedPosition());
        assertEquals(1, dto.asks().size());
        assertEquals(101L, dto.asks().get(0).price());
        assertEquals(4L, dto.asks().get(0).size());
        assertEquals(1, dto.asks().get(0).orders());
        assertEquals(2L, dto.bids().get(0).size());
    }

    @Test
    void mapsOrderWithLifecycleAndRemaining() {
        final OrderRecordResult order = new OrderRecordResult(
                1,
                7L,
                811L,
                true,
                "GTC",
                100L,
                10L,
                6L,
                0L,
                1_000L,
                2_000L,
                111,
                OrderState.ACTIVE,
                "ACTIVE",
                List.of(new OrderRecordResult.FillResult(false, 100L, 6L, 812L, 1_500L)));
        final OrderDto dto = Mapper.order(order);
        assertEquals("ASK", dto.side());
        assertEquals("GTC", dto.orderType());
        assertEquals(4L, dto.remaining());
        assertEquals("ACTIVE", dto.state());
        assertEquals(1, dto.fills().size());
        assertEquals(812L, dto.fills().get(0).counterpartyUid());
    }

    @Test
    void mapsUserReport() {
        final UserReport report = new UserReport(
                811L,
                true,
                false,
                42,
                List.of(new UserReport.Balance(10, 990L), new UserReport.Balance(20, 600L)),
                List.of(new UserReport.RestingOrder(1, 7L, true, 101L, 10L, 6L, 0L)));
        final UserReportDto dto = Mapper.userReport(report);
        assertTrue(dto.exists());
        assertFalse(dto.suspended());
        assertEquals(2, dto.balances().size());
        assertEquals(990L, dto.balances().get(0).balance());
        assertEquals(1, dto.orders().size());
        assertEquals("ASK", dto.orders().get(0).side());
        assertEquals(4L, dto.orders().get(0).remaining());
    }

    @Test
    void mapsTotalsAndTrade() {
        final TotalBalanceResult totals =
                new TotalBalanceResult(42, List.of(new TotalBalanceResult.Total(10, 990L, 10L, 0L)));
        final TotalBalanceDto dto = Mapper.totalBalance(totals);
        assertEquals(1_000L, dto.totals().get(0).total());

        final MarketTradeResult trade = new MarketTradeResult(1_500L, 1, 100L, 6L, 7L, 811L, 812L);
        final TradeDto tradeDto = Mapper.trade(trade);
        assertEquals(7L, tradeDto.makerOrderId());
        assertEquals(811L, tradeDto.makerUid());
        assertEquals(812L, tradeDto.takerUid());
    }
}
