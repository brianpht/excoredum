package io.justrade.read.order;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * One order's lifecycle rebuilt deterministically from the replicated command
 * log by {@link OrderLedger}: placement fields, current state, executed
 * fills, and the leader-assigned timestamps of placement and last change.
 * Placement fields are immutable; the ledger mutates state, fills, and
 * timestamps on the replica's single polling thread, so readers always see a
 * consistent snapshot.
 */
public final class OrderRecord {

    /** State: placement applied, result not yet seen (never survives one command's outcome). */
    public static final int STATE_NEW = 0;

    /** State: resting after a successful place. */
    public static final int STATE_ACTIVE = 1;

    /** State: fully cancelled or reduced. */
    public static final int STATE_CANCELLED = 2;

    /** State: fully filled. */
    public static final int STATE_COMPLETED = 3;

    /** State: placement rejected, or unmatched remainder rejected (IOC / FOK). */
    public static final int STATE_REJECTED = 4;

    private final long orderId;
    private final long uid;
    private final int symbolId;
    private final boolean ask;
    private final String orderType;
    private final int userCookie;
    private final long size;
    private final long placedTimestamp;
    private final ArrayList<Fill> fills = new ArrayList<>(2);
    private long price;
    private long filled;
    private long reduced;
    private long lastTimestamp;
    private int state;

    public OrderRecord(
            final long orderId,
            final long uid,
            final int symbolId,
            final boolean ask,
            final String orderType,
            final int userCookie,
            final long price,
            final long size,
            final long placedTimestamp) {
        this.orderId = orderId;
        this.uid = uid;
        this.symbolId = symbolId;
        this.ask = ask;
        this.orderType = orderType;
        this.userCookie = userCookie;
        this.price = price;
        this.size = size;
        this.placedTimestamp = placedTimestamp;
        this.lastTimestamp = placedTimestamp;
    }

    public long orderId() {
        return orderId;
    }

    public long uid() {
        return uid;
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

    public int userCookie() {
        return userCookie;
    }

    /** The order's price, or the FOK budget; updated on a successful move. */
    public long price() {
        return price;
    }

    /** The size as originally placed. */
    public long size() {
        return size;
    }

    /** Total size executed across all fills. */
    public long filled() {
        return filled;
    }

    /** Total size explicitly reduced (cancel / reduce commands). */
    public long reduced() {
        return reduced;
    }

    /** Remaining size: {@code size - filled - reduced}. */
    public long remaining() {
        return size - filled - reduced;
    }

    public int state() {
        return state;
    }

    public long placedTimestamp() {
        return placedTimestamp;
    }

    public long lastTimestamp() {
        return lastTimestamp;
    }

    /** Every executed fill, in matching order. */
    public List<Fill> fills() {
        return fills;
    }

    /** The REST name of the current state. */
    public String stateName() {
        return stateName(state);
    }

    /** Maps a state constant to its name. */
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

    void price(final long value) {
        price = value;
    }

    void state(final int value) {
        state = value;
    }

    /** Records one executed fill; a fill that exhausts the order completes it. */
    void addFill(
            final boolean taker, final long fillPrice, final long fillSize, final long counterpartyUid, final long ts) {
        fills.add(new Fill(taker, fillPrice, fillSize, counterpartyUid, ts));
        filled += fillSize;
        lastTimestamp = ts;
        if (state == STATE_ACTIVE && filled >= size) {
            state = STATE_COMPLETED;
        }
    }

    /** Records an explicit reduce (cancel / reduce command) of {@code reducedBy}. */
    void addReduce(final long reducedBy, final long ts) {
        reduced += reducedBy;
        lastTimestamp = ts;
    }

    /** Marks the last change time without altering state. */
    void touch(final long ts) {
        lastTimestamp = ts;
    }

    /** Restores the mutable fields from a checkpoint load (see {@link #writeTo}). */
    void restore(
            final long restoredPrice,
            final long restoredFilled,
            final long restoredReduced,
            final long restoredLastTimestamp,
            final int restoredState,
            final List<Fill> restoredFills) {
        this.price = restoredPrice;
        this.filled = restoredFilled;
        this.reduced = restoredReduced;
        this.lastTimestamp = restoredLastTimestamp;
        this.state = restoredState;
        this.fills.addAll(restoredFills);
    }

    /** Writes this record (placement fields, mutable state, fills) to {@code out}. */
    void writeTo(final DataOutput out) throws IOException {
        out.writeLong(orderId);
        out.writeLong(uid);
        out.writeInt(symbolId);
        out.writeBoolean(ask);
        out.writeUTF(orderType);
        out.writeInt(userCookie);
        out.writeLong(price);
        out.writeLong(size);
        out.writeLong(filled);
        out.writeLong(reduced);
        out.writeLong(placedTimestamp);
        out.writeLong(lastTimestamp);
        out.writeInt(state);
        out.writeInt(fills.size());
        for (final Fill fill : fills) {
            out.writeBoolean(fill.taker());
            out.writeLong(fill.price());
            out.writeLong(fill.size());
            out.writeLong(fill.counterpartyUid());
            out.writeLong(fill.timestamp());
        }
    }

    /** Reads a record previously written by {@link #writeTo}. */
    static OrderRecord readFrom(final DataInput in) throws IOException {
        final long orderId = in.readLong();
        final long uid = in.readLong();
        final int symbolId = in.readInt();
        final boolean ask = in.readBoolean();
        final String orderType = in.readUTF();
        final int userCookie = in.readInt();
        final long price = in.readLong();
        final long size = in.readLong();
        final long filled = in.readLong();
        final long reduced = in.readLong();
        final long placedTimestamp = in.readLong();
        final long lastTimestamp = in.readLong();
        final int state = in.readInt();
        final int fillCount = in.readInt();
        final ArrayList<Fill> restored = new ArrayList<>(fillCount);
        for (int i = 0; i < fillCount; i++) {
            restored.add(new Fill(in.readBoolean(), in.readLong(), in.readLong(), in.readLong(), in.readLong()));
        }
        final OrderRecord record =
                new OrderRecord(orderId, uid, symbolId, ask, orderType, userCookie, price, size, placedTimestamp);
        record.restore(price, filled, reduced, lastTimestamp, state, restored);
        return record;
    }
}
