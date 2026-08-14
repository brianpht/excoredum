package com.exadbe.read.client;

import java.util.List;

/**
 * One order's lifecycle as rebuilt by the read replica, mirrored from the
 * replica's {@code OrderRecord}. Placement fields are immutable; state, fills,
 * and timestamps reflect the replicated log at the answered position.
 */
public record OrderRecordResult(
        int symbolId,
        long orderId,
        long uid,
        boolean ask,
        String orderType,
        long price,
        long size,
        long filled,
        long reduced,
        long placedTimestamp,
        long lastTimestamp,
        int userCookie,
        int state,
        String stateName,
        List<FillResult> fills) {

    /** Remaining size: {@code size - filled - reduced}. */
    public long remaining() {
        return size - filled - reduced;
    }

    /** One executed fill against a resting counterparty. */
    public record FillResult(boolean taker, long price, long size, long counterpartyUid, long timestamp) {}
}
