package com.exadbe.core;

import com.exadbe.protocol.CommandResultCode;

/**
 * Mutable, reusable holder for the outcome of a single command.
 *
 * <p>A single instance is owned by the service and reset before each dispatch,
 * so handlers never allocate a result object on the hot path. It carries the
 * command identity so the value can be cached for dedup and re-sent verbatim on
 * a duplicate submission.
 *
 * <p>It also accumulates the matcher events a command produced (trade / reduce /
 * reject) into a reusable, growable buffer, so the service can encode egress
 * event frames without a per-event allocation in steady state.
 */
public final class CommandOutcome {

    /** The kind of a recorded matcher event; determines how its fields are read. */
    public enum EventKind {
        TRADE,
        REDUCE,
        REJECT
    }

    /**
     * One recorded matcher event, reused across commands. Field meaning depends
     * on {@link #kind()}:
     *
     * <ul>
     *   <li>{@code TRADE}: {@code makerOrderId}, {@code makerUid},
     *       {@code takerUid}, {@code price}, {@code size}, {@code makerCompleted}.
     *   <li>{@code REDUCE}: {@code orderId}={@code makerOrderId},
     *       {@code uid}={@code makerUid}, {@code size}=reduced-by,
     *       {@code price}=resting price, {@code makerCompleted}=order-completed.
     *   <li>{@code REJECT}: {@code orderId}={@code makerOrderId},
     *       {@code uid}={@code makerUid}, {@code size}=rejected-size,
     *       {@code price}=active order price (budget for FOK-BUDGET).
     * </ul>
     */
    public static final class EventRecord {
        private EventKind kind;
        private int symbolId;
        private long makerOrderId;
        private long makerUid;
        private long takerUid;
        private long price;
        private long size;
        private boolean makerCompleted;
        private boolean makerBid;
        private long makerReserveBidPrice;
        private boolean takerBid;
        private long takerReserveBidPrice;

        public EventKind kind() {
            return kind;
        }

        public int symbolId() {
            return symbolId;
        }

        public long makerOrderId() {
            return makerOrderId;
        }

        public long makerUid() {
            return makerUid;
        }

        public long takerUid() {
            return takerUid;
        }

        public long price() {
            return price;
        }

        public long size() {
            return size;
        }

        public boolean makerCompleted() {
            return makerCompleted;
        }

        public boolean makerBid() {
            return makerBid;
        }

        public long makerReserveBidPrice() {
            return makerReserveBidPrice;
        }

        public boolean takerBid() {
            return takerBid;
        }

        public long takerReserveBidPrice() {
            return takerReserveBidPrice;
        }
    }

    private static final int INITIAL_EVENT_CAPACITY = 64;

    private long commandIdHi;
    private long commandIdLo;
    private CommandResultCode resultCode = CommandResultCode.SUCCESS;
    private long uid;
    private boolean hasUid;
    private long orderId;
    private boolean hasOrderId;
    private long filledSize;
    private boolean hasFilledSize;

    private EventRecord[] events = newEventArray(INITIAL_EVENT_CAPACITY);
    private int eventCount;
    private boolean grewEventBuffer;

    private static EventRecord[] newEventArray(final int capacity) {
        final EventRecord[] array = new EventRecord[capacity];
        for (int i = 0; i < capacity; i++) {
            array[i] = new EventRecord();
        }
        return array;
    }

    /** Creates an outcome with the default preallocated event capacity. */
    public CommandOutcome() {
        this(INITIAL_EVENT_CAPACITY);
    }

    /** Creates an outcome whose event buffer is preallocated to {@code eventCapacity}. */
    public CommandOutcome(final int eventCapacity) {
        if (eventCapacity != INITIAL_EVENT_CAPACITY) {
            this.events = newEventArray(Math.max(1, eventCapacity));
        }
    }

    /** Clears all fields and records the command identity for the next dispatch. */
    public void reset(final long idHi, final long idLo) {
        this.commandIdHi = idHi;
        this.commandIdLo = idLo;
        this.resultCode = CommandResultCode.SUCCESS;
        this.uid = 0L;
        this.hasUid = false;
        this.orderId = 0L;
        this.hasOrderId = false;
        this.filledSize = 0L;
        this.hasFilledSize = false;
        this.eventCount = 0;
        this.grewEventBuffer = false;
    }

    /** Copies identity and result fields from a cached dedup record. */
    public void set(
            final long idHi,
            final long idLo,
            final CommandResultCode cachedCode,
            final long cachedUid,
            final boolean uidPresent,
            final long cachedOrderId,
            final boolean orderIdPresent,
            final long cachedFilledSize,
            final boolean filledSizePresent) {
        this.commandIdHi = idHi;
        this.commandIdLo = idLo;
        this.resultCode = cachedCode;
        this.uid = cachedUid;
        this.hasUid = uidPresent;
        this.orderId = cachedOrderId;
        this.hasOrderId = orderIdPresent;
        this.filledSize = cachedFilledSize;
        this.hasFilledSize = filledSizePresent;
        this.eventCount = 0;
    }

    /** Records a trade fill against a resting maker order. */
    public void addTrade(
            final int symbolId,
            final long makerOrderId,
            final long makerUid,
            final long takerUid,
            final long price,
            final long size,
            final boolean makerCompleted,
            final boolean makerBid,
            final long makerReserveBidPrice,
            final boolean takerBid,
            final long takerReserveBidPrice) {
        final EventRecord e = nextEvent();
        e.kind = EventKind.TRADE;
        e.symbolId = symbolId;
        e.makerOrderId = makerOrderId;
        e.makerUid = makerUid;
        e.takerUid = takerUid;
        e.price = price;
        e.size = size;
        e.makerCompleted = makerCompleted;
        e.makerBid = makerBid;
        e.makerReserveBidPrice = makerReserveBidPrice;
        e.takerBid = takerBid;
        e.takerReserveBidPrice = takerReserveBidPrice;
    }

    /** Records a reduce (or cancel) of a resting order; carries its risk context. */
    public void addReduce(
            final int symbolId,
            final long orderId,
            final long uid,
            final long reducedBy,
            final boolean bid,
            final long reserveBidPrice,
            final long price,
            final boolean orderCompleted) {
        final EventRecord e = nextEvent();
        e.kind = EventKind.REDUCE;
        e.symbolId = symbolId;
        e.makerOrderId = orderId;
        e.makerUid = uid;
        e.size = reducedBy;
        e.makerBid = bid;
        e.makerReserveBidPrice = reserveBidPrice;
        e.price = price;
        e.makerCompleted = orderCompleted;
    }

    /** Records rejected (unmatched) size at the active order's {@code price}. */
    public void addReject(
            final int symbolId, final long orderId, final long uid, final long rejectedSize, final long price) {
        final EventRecord e = nextEvent();
        e.kind = EventKind.REJECT;
        e.symbolId = symbolId;
        e.makerOrderId = orderId;
        e.makerUid = uid;
        e.size = rejectedSize;
        e.price = price;
    }

    /** Number of matcher events recorded for the command just processed. */
    public int eventCount() {
        return eventCount;
    }

    /** Returns the event at {@code index} in emission order. */
    public EventRecord event(final int index) {
        return events[index];
    }

    private EventRecord nextEvent() {
        if (eventCount == events.length) {
            grow();
        }
        return events[eventCount++];
    }

    // Cold path: a single command swept more makers than the current buffer holds.
    private void grow() {
        final int oldLength = events.length;
        final EventRecord[] larger = new EventRecord[oldLength * 2];
        System.arraycopy(events, 0, larger, 0, oldLength);
        for (int i = oldLength; i < larger.length; i++) {
            larger[i] = new EventRecord();
        }
        events = larger;
        grewEventBuffer = true;
    }

    /** True if the last command overflowed the preallocated buffer, forcing a grow. */
    public boolean grewEventBuffer() {
        return grewEventBuffer;
    }

    public void resultCode(final CommandResultCode code) {
        this.resultCode = code;
    }

    public void uid(final long value) {
        this.uid = value;
        this.hasUid = true;
    }

    public void orderId(final long value) {
        this.orderId = value;
        this.hasOrderId = true;
    }

    public void filledSize(final long value) {
        this.filledSize = value;
        this.hasFilledSize = true;
    }

    public long commandIdHi() {
        return commandIdHi;
    }

    public long commandIdLo() {
        return commandIdLo;
    }

    public CommandResultCode resultCode() {
        return resultCode;
    }

    public long uid() {
        return uid;
    }

    public boolean hasUid() {
        return hasUid;
    }

    public long orderId() {
        return orderId;
    }

    public boolean hasOrderId() {
        return hasOrderId;
    }

    public long filledSize() {
        return filledSize;
    }

    public boolean hasFilledSize() {
        return hasFilledSize;
    }
}
