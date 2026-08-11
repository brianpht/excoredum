package com.exadbe.gateway.transport;

import io.netty.channel.ChannelHandlerContext;

/**
 * A pooled REST request descriptor handed from a Netty event loop to the
 * gateway agent over a lock-free queue. Exactly one thread owns a slot at a
 * time: an event loop fills it, the agent drains and answers it, then returns
 * it to the free queue. All mutable fields are reset on release.
 *
 * <p>Decimal request fields travel as raw text (e.g. {@code priceText}) and
 * are converted to scaled longs by the agent, which owns the asset/symbol
 * scales.
 */
public final class GatewayRequest {

    /** Request kind: GET ping. */
    public static final int PING = 1;

    /** Request kind: GET server time. */
    public static final int TIME = 2;

    /** Request kind: GET exchange info. */
    public static final int INFO = 3;

    /** Request kind: POST create user. */
    public static final int CREATE_USER = 4;

    /** Request kind: POST balance adjustment. */
    public static final int ADJUST_BALANCE = 5;

    /** Request kind: POST register asset (gateway-local). */
    public static final int CREATE_ASSET = 6;

    /** Request kind: POST register symbol (validated, then ADD_SYMBOL). */
    public static final int CREATE_SYMBOL = 7;

    /** Request kind: GET L2 order book from the read replica. */
    public static final int ORDER_BOOK = 8;

    /** Request kind: POST place order. */
    public static final int PLACE_ORDER = 9;

    /** Request kind: PUT move order. */
    public static final int MOVE_ORDER = 10;

    /** Request kind: DELETE cancel order. */
    public static final int CANCEL_ORDER = 11;

    /** Request kind: GET user state from the read replica. */
    public static final int USER_STATE = 12;

    /** Request kind: GET user order history from the gateway profile. */
    public static final int USER_HISTORY = 13;

    /** Sentinel marking an absent long field. */
    public static final long ABSENT = Long.MIN_VALUE;

    public int kind;
    public ChannelHandlerContext ctx;
    public boolean keepAlive;
    public long deadlineNanos;

    // Path and query parameters.
    public long uid = ABSENT;
    public String symbolCode;
    public long orderId = ABSENT;
    public int depth;

    // Body fields: longs.
    public long transactionId = ABSENT;
    public long size = ABSENT;
    public long userCookie = ABSENT;
    public int assetId = -1;
    public int symbolId = -1;
    public int scale = -1;

    // Body fields: strings.
    public String assetCode;
    public String currencyCode;
    public String symbolType;
    public String baseAsset;
    public String quoteCurrency;
    public String action;
    public String orderType;

    // Body fields: raw decimal text, scaled by the agent.
    public String priceText;
    public String budgetText;
    public String amountText;
    public String lotSizeText;
    public String stepSizeText;
    public String takerFeeText;
    public String makerFeeText;
    public String marginBuyText;
    public String marginSellText;

    // Agent-side scratch: scaled values parsed from the text fields above.
    public long price = ABSENT;
    public long budget = ABSENT;
    public long amount = ABSENT;

    // Submission correlation and intrusive in-flight list links.
    public long commandIdLo;
    public GatewayRequest prevInFlight;
    public GatewayRequest nextInFlight;

    // Reusable body buffer, grown only when a larger request arrives.
    public byte[] bodyBuffer = new byte[0];

    /** Clears all fields for reuse; retains the body buffer. */
    public void reset() {
        kind = 0;
        ctx = null;
        keepAlive = false;
        deadlineNanos = 0L;
        uid = ABSENT;
        symbolCode = null;
        orderId = ABSENT;
        depth = 0;
        transactionId = ABSENT;
        size = ABSENT;
        userCookie = ABSENT;
        assetId = -1;
        symbolId = -1;
        scale = -1;
        assetCode = null;
        currencyCode = null;
        symbolType = null;
        baseAsset = null;
        quoteCurrency = null;
        action = null;
        orderType = null;
        priceText = null;
        budgetText = null;
        amountText = null;
        lotSizeText = null;
        stepSizeText = null;
        takerFeeText = null;
        makerFeeText = null;
        marginBuyText = null;
        marginSellText = null;
        price = ABSENT;
        budget = ABSENT;
        amount = ABSENT;
        commandIdLo = 0L;
        prevInFlight = null;
        nextInFlight = null;
    }
}
