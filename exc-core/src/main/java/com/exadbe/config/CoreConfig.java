package com.exadbe.config;

/**
 * Immutable core configuration: preallocated capacities and tuning knobs.
 *
 * <p>All capacities are sized at construction; the engine never grows a
 * structure during the operational window.
 */
public final class CoreConfig {

    /** Default number of symbols preallocated. */
    public static final int DEFAULT_SYMBOL_CAPACITY = 1024;

    /** Default number of user accounts preallocated. */
    public static final int DEFAULT_ACCOUNT_CAPACITY = 1 << 16;

    /** Default number of distinct clients tracked for dedup. */
    public static final int DEFAULT_DEDUP_CLIENT_CAPACITY = 1 << 12;

    /** Default per-client dedup window (retained recent client sequences). */
    public static final int DEFAULT_DEDUP_WINDOW = 1 << 10;

    /** Default resting-order pool capacity (retained free nodes). */
    public static final int DEFAULT_ORDER_POOL_CAPACITY = 1 << 16;

    /** Default maximum L2 depth returned per side for an order-book request. */
    public static final int DEFAULT_L2_MAX_LEVELS = 32;

    /** Default preallocated matcher-event buffer capacity per command. */
    public static final int DEFAULT_EVENT_BUFFER_CAPACITY = 1 << 10;

    /** Default number of slots in the domain-event journal ring (power of two). */
    public static final int DEFAULT_JOURNAL_SLOT_COUNT = 1 << 16;

    /** Default size in bytes of one journal ring slot. */
    public static final int DEFAULT_JOURNAL_SLOT_SIZE = 128;

    private final int symbolCapacity;
    private final int accountCapacity;
    private final int dedupClientCapacity;
    private final int dedupWindow;
    private final int orderPoolCapacity;
    private final int l2MaxLevels;
    private final int eventBufferCapacity;
    private final int journalSlotCount;
    private final int journalSlotSize;

    private CoreConfig(
            final int symbolCapacity,
            final int accountCapacity,
            final int dedupClientCapacity,
            final int dedupWindow,
            final int orderPoolCapacity,
            final int l2MaxLevels,
            final int eventBufferCapacity,
            final int journalSlotCount,
            final int journalSlotSize) {
        this.symbolCapacity = symbolCapacity;
        this.accountCapacity = accountCapacity;
        this.dedupClientCapacity = dedupClientCapacity;
        this.dedupWindow = dedupWindow;
        this.orderPoolCapacity = orderPoolCapacity;
        this.l2MaxLevels = l2MaxLevels;
        this.eventBufferCapacity = eventBufferCapacity;
        this.journalSlotCount = journalSlotCount;
        this.journalSlotSize = journalSlotSize;
    }

    public static CoreConfig defaults() {
        return new CoreConfig(
                DEFAULT_SYMBOL_CAPACITY,
                DEFAULT_ACCOUNT_CAPACITY,
                DEFAULT_DEDUP_CLIENT_CAPACITY,
                DEFAULT_DEDUP_WINDOW,
                DEFAULT_ORDER_POOL_CAPACITY,
                DEFAULT_L2_MAX_LEVELS,
                DEFAULT_EVENT_BUFFER_CAPACITY,
                DEFAULT_JOURNAL_SLOT_COUNT,
                DEFAULT_JOURNAL_SLOT_SIZE);
    }

    public int symbolCapacity() {
        return symbolCapacity;
    }

    public int accountCapacity() {
        return accountCapacity;
    }

    public int dedupClientCapacity() {
        return dedupClientCapacity;
    }

    public int dedupWindow() {
        return dedupWindow;
    }

    public int orderPoolCapacity() {
        return orderPoolCapacity;
    }

    public int l2MaxLevels() {
        return l2MaxLevels;
    }

    public int eventBufferCapacity() {
        return eventBufferCapacity;
    }

    public int journalSlotCount() {
        return journalSlotCount;
    }

    public int journalSlotSize() {
        return journalSlotSize;
    }
}
