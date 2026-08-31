package io.justrade.gateway.dto;

/** One resting order reported by a {@code singleUserReport}. */
public record RestingOrderDto(
        int symbolId,
        long orderId,
        String side,
        long price,
        long size,
        long filled,
        long remaining,
        long reserveBidPrice) {}
