package com.exadbe.gateway.dto;

/** JSON body for {@code POST /api/v1/orders}. {@code type} is GTC / IOC / FOK_BUDGET. */
public record PlaceOrderRequest(
        int symbolId,
        long orderId,
        boolean ask,
        String type,
        long price,
        long size,
        long reserveBidPrice,
        long uid,
        int userCookie) {}
