package com.exadbe.gateway.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Gateway-side record of one order placed through this gateway instance, kept
 * for the user history endpoint. Owned exclusively by the gateway agent
 * thread. Fills and state transitions arrive through the client's egress
 * listeners; orders placed through other gateways or raw clients are not
 * tracked here.
 */
public final class GatewayOrder {

    /** State: submitted, no result yet. */
    public static final int STATE_NEW = 0;

    /** State: resting after a successful place. */
    public static final int STATE_ACTIVE = 1;

    /** State: cancelled (fully reduced). */
    public static final int STATE_CANCELLED = 2;

    /** State: fully filled. */
    public static final int STATE_COMPLETED = 3;

    /** State: unmatched remainder rejected (IOC / FOK). */
    public static final int STATE_REJECTED = 4;

    private final long orderId;
    private final String symbolCode;
    private final int symbolId;
    private final boolean ask;
    private final String orderType;
    private final long userCookie;
    private final long commandIdLo;
    private long price;
    private long size;
    private long filled;
    private int state;
    private final ArrayList<Deal> deals = new ArrayList<>(2);

    /** One fill recorded against this order. */
    public record Deal(boolean taker, long price, long size) {}

    public GatewayOrder(
            final long orderId,
            final String symbolCode,
            final int symbolId,
            final boolean ask,
            final String orderType,
            final long userCookie,
            final long price,
            final long size,
            final int state,
            final long commandIdLo) {
        this.orderId = orderId;
        this.symbolCode = symbolCode;
        this.symbolId = symbolId;
        this.ask = ask;
        this.orderType = orderType;
        this.userCookie = userCookie;
        this.price = price;
        this.size = size;
        this.state = state;
        this.commandIdLo = commandIdLo;
    }

    public long orderId() {
        return orderId;
    }

    public String symbolCode() {
        return symbolCode;
    }

    public int symbolId() {
        return symbolId;
    }

    public boolean ask() {
        return ask;
    }

    public String orderType() {
        return orderType;
    }

    public long userCookie() {
        return userCookie;
    }

    /** The command id this order was placed under, for egress event correlation. */
    public long commandIdLo() {
        return commandIdLo;
    }

    public long price() {
        return price;
    }

    public void price(final long value) {
        this.price = value;
    }

    public long size() {
        return size;
    }

    public void size(final long value) {
        this.size = value;
    }

    public long filled() {
        return filled;
    }

    public int state() {
        return state;
    }

    public void state(final int value) {
        this.state = value;
    }

    /** Records one fill against this order, advancing {@link #filled()}. */
    public void addDeal(final boolean takerRole, final long dealPrice, final long dealSize) {
        filled += dealSize;
        deals.add(new Deal(takerRole, dealPrice, dealSize));
    }

    public List<Deal> deals() {
        return deals;
    }

    /** The REST name of the current state. */
    public String stateName() {
        return stateName(state);
    }

    /** Maps a state constant to its REST name. */
    public static String stateName(final int state) {
        switch (state) {
            case STATE_NEW:
                return "NEW";
            case STATE_ACTIVE:
                return "ACTIVE";
            case STATE_CANCELLED:
                return "CANCELLED";
            case STATE_COMPLETED:
                return "COMPLETED";
            case STATE_REJECTED:
                return "REJECTED";
            default:
                return "UNKNOWN";
        }
    }
}
