package io.justrade.gateway.dto;

/** JSON body for the admin {@code POST /api/v1/users/{uid}/balance} endpoint. */
public record AdjustBalanceRequest(Integer currency, Long amount) {}
