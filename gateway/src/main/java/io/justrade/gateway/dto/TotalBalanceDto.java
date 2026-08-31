package io.justrade.gateway.dto;

import java.util.List;

/** Per-currency conservation totals (read-side {@code totalCurrencyBalance}). */
public record TotalBalanceDto(long appliedPosition, List<TotalDto> totals) {}
