package com.exadbe.read.order;

/** One executed trade recorded on the read side, from a replicated TRADE event. */
public record Fill(boolean taker, long price, long size, long counterpartyUid, long timestamp) {}
