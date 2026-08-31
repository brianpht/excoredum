package com.exadbe.gateway.dto;

import java.util.List;

/** Replica health, the deterministic state hash, client stats, and conservation totals. */
public record HealthDto(
        long appliedPosition,
        long stateHash,
        long submitted,
        long completed,
        long expired,
        long backpressure,
        boolean ready,
        List<TotalDto> totals) {}
