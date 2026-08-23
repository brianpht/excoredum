package com.exadbe.gateway.dto;

/**
 * JSON body for {@code PATCH /api/v1/orders/{orderId}}. Exactly one of
 * {@code price} (move) or {@code size} (reduce) must be present.
 */
public record ModifyOrderRequest(int symbolId, long uid, Long price, Long size) {}
