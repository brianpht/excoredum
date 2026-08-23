package com.exadbe.gateway.dto;

/**
 * The deterministic result of one write command. {@code uid}, {@code orderId},
 * and {@code filledSize} are nullable (absent from the wire result, they are
 * {@code null} in JSON).
 */
public record WriteResultDto(
        long commandIdHi, long commandIdLo, String resultCode, Long uid, Long orderId, Long filledSize) {}
