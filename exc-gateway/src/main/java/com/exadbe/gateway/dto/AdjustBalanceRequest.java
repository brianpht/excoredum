package com.exadbe.gateway.dto;

/** JSON body for the admin {@code POST /api/v1/users/{uid}/balance} endpoint. */
public record AdjustBalanceRequest(int currency, long amount) {}
