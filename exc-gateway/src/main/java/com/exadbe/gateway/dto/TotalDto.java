package com.exadbe.gateway.dto;

/** One currency's value-conservation breakdown. */
public record TotalDto(int currency, long accountBalance, long reserved, long fees, long total) {}
