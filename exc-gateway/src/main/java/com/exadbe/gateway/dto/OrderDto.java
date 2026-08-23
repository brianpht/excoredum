package com.exadbe.gateway.dto;

import java.util.List;

/** One order's lifecycle, shaped for the UI (read-side order / active / history). */
public record OrderDto(
        int symbolId,
        long orderId,
        long uid,
        String side,
        String orderType,
        long price,
        long size,
        long filled,
        long reduced,
        long remaining,
        long placedTimestamp,
        long lastTimestamp,
        int userCookie,
        String state,
        List<FillDto> fills) {

    /** One executed fill. Section discarded the counterparty side for brevity. */
    public record FillDto(boolean taker, long price, long size, long counterpartyUid, long timestamp) {}
}
