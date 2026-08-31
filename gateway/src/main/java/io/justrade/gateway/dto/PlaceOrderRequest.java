package io.justrade.gateway.dto;

/**
 * JSON body for {@code POST /api/v1/orders}. {@code type} is GTC / IOC / FOK_BUDGET.
 * The identity and quantity fields are boxed so an absent field is a {@code null}
 * the router can reject with a 400 instead of silently deserializing to zero.
 */
public record PlaceOrderRequest(
        Integer symbolId,
        Long orderId,
        Boolean ask,
        String type,
        Long price,
        Long size,
        long reserveBidPrice,
        Long uid,
        int userCookie) {}
