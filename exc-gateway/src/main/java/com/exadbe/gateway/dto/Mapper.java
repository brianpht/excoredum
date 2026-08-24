package com.exadbe.gateway.dto;

import com.exadbe.read.client.L2Snapshot;
import com.exadbe.read.client.MarketTradeResult;
import com.exadbe.read.client.OrderRecordResult;
import com.exadbe.read.client.TotalBalanceResult;
import com.exadbe.read.client.UserReport;

/**
 * Maps read-side result holders to gateway DTOs. Sits at the HTTP boundary only;
 * it never reaches {@code exc-core}. The wire uses BID/ASK terminology, so the
 * DTOs carry {@code "BID"} / {@code "ASK"} for order side.
 */
public final class Mapper {

    private Mapper() {}

    private static String side(final boolean ask) {
        return ask ? "ASK" : "BID";
    }

    public static OrderBookDto orderBook(final L2Snapshot snapshot) {
        // A symbol the engine does not know reports found=false; its levels are
        // not meaningful (the read side may return uninitialised buckets under
        // concurrent in-flight queries), so surface an empty book instead.
        final boolean found = snapshot.found();
        return new OrderBookDto(
                snapshot.symbolId(),
                found,
                snapshot.appliedPosition(),
                found
                        ? snapshot.asks().stream()
                                .map(l -> new OrderBookDto.Level(l.price(), l.size(), l.orders()))
                                .toList()
                        : java.util.List.of(),
                found
                        ? snapshot.bids().stream()
                                .map(l -> new OrderBookDto.Level(l.price(), l.size(), l.orders()))
                                .toList()
                        : java.util.List.of());
    }

    public static TradeDto trade(final MarketTradeResult trade) {
        return new TradeDto(
                trade.timestamp(),
                trade.symbolId(),
                trade.price(),
                trade.size(),
                trade.makerOrderId(),
                trade.makerUid(),
                trade.takerUid());
    }

    public static OrderDto order(final OrderRecordResult order) {
        return new OrderDto(
                order.symbolId(),
                order.orderId(),
                order.uid(),
                side(order.ask()),
                order.orderType(),
                order.price(),
                order.size(),
                order.filled(),
                order.reduced(),
                order.remaining(),
                order.placedTimestamp(),
                order.lastTimestamp(),
                order.userCookie(),
                order.stateName(),
                order.fills().stream().map(Mapper::fill).toList());
    }

    private static OrderDto.FillDto fill(final OrderRecordResult.FillResult fill) {
        return new OrderDto.FillDto(fill.taker(), fill.price(), fill.size(), fill.counterpartyUid(), fill.timestamp());
    }

    private static RestingOrderDto resting(final UserReport.RestingOrder order) {
        return new RestingOrderDto(
                order.symbolId(),
                order.orderId(),
                side(order.ask()),
                order.price(),
                order.size(),
                order.filled(),
                order.size() - order.filled(),
                order.reserveBidPrice());
    }

    public static UserReportDto userReport(final UserReport report) {
        return new UserReportDto(
                report.uid(),
                report.exists(),
                report.suspended(),
                report.appliedPosition(),
                report.balances().stream()
                        .map(b -> new BalanceDto(b.currency(), b.balance()))
                        .toList(),
                report.orders().stream().map(Mapper::resting).toList());
    }

    public static TotalBalanceDto totalBalance(final TotalBalanceResult totals) {
        return new TotalBalanceDto(
                totals.appliedPosition(),
                totals.totals().stream().map(Mapper::total).toList());
    }

    private static TotalDto total(final TotalBalanceResult.Total total) {
        return new TotalDto(total.currency(), total.accountBalance(), total.reserved(), total.fees(), total.total());
    }
}
