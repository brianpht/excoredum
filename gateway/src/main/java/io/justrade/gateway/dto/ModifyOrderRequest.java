package io.justrade.gateway.dto;

/**
 * JSON body for {@code PATCH /api/v1/orders/{orderId}}. Exactly one of
 * {@code price} (move) or {@code size} (reduce) must be present.
 */
public record ModifyOrderRequest(Integer symbolId, Long uid, Long price, Long size) {}
