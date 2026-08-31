package io.justrade.gateway.dto;

import java.util.List;

/** User status, balances, and resting orders (read-side {@code singleUserReport}). */
public record UserReportDto(
        long uid,
        boolean exists,
        boolean suspended,
        long appliedPosition,
        List<BalanceDto> balances,
        List<RestingOrderDto> orders) {}
