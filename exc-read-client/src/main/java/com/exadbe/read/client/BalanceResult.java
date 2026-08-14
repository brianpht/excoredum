package com.exadbe.read.client;

/** Result of a {@code BALANCE} query: the balance and whether the account exists. */
public record BalanceResult(long balance, boolean found, long appliedPosition) {}
