package io.justrade.config;

import java.util.Properties;

/**
 * Immutable core configuration: preallocated capacities and tuning knobs.
 *
 * <p>All capacities are sized at construction; the engine never grows a
 * structure during the operational window.
 */
public final class CoreConfig {

    /** Property prefix for the operator-facing overrides read by {@link #fromProperties}. */
    public static final String PROPERTY_PREFIX = "justrade.core.";

    /** Default number of user accounts preallocated. */
    public static final int DEFAULT_ACCOUNT_CAPACITY = 1 << 16;

    /** Default number of distinct clients tracked for dedup. */
    public static final int DEFAULT_DEDUP_CLIENT_CAPACITY = 1 << 12;

    /** Default per-client dedup window (retained recent client sequences). */
    public static final int DEFAULT_DEDUP_WINDOW = 1 << 10;

    /** Default resting-order pool capacity (retained free nodes). */
    public static final int DEFAULT_ORDER_POOL_CAPACITY = 1 << 16;

    /**
     * Default price-bucket pool capacity (retained free price levels). A price
     * level aggregates many orders, so far fewer buckets than order nodes are
     * live at once; 2^13 covers thousands of concurrent levels across all
     * symbols at a few hundred KB.
     */
    public static final int DEFAULT_PRICE_BUCKET_CAPACITY = 1 << 13;

    /** Default maximum L2 depth returned per side for an order-book request. */
    public static final int DEFAULT_L2_MAX_LEVELS = 32;

    /** Default preallocated matcher-event buffer capacity per command. */
    public static final int DEFAULT_EVENT_BUFFER_CAPACITY = 1 << 10;

    /** Default number of slots in the domain-event journal ring (power of two). */
    public static final int DEFAULT_JOURNAL_SLOT_COUNT = 1 << 16;

    /** Default size in bytes of one journal ring slot. */
    public static final int DEFAULT_JOURNAL_SLOT_SIZE = 128;

    private final int accountCapacity;
    private final int dedupClientCapacity;
    private final int dedupWindow;
    private final int orderPoolCapacity;
    private final int priceBucketCapacity;
    private final int l2MaxLevels;
    private final int eventBufferCapacity;
    private final int journalSlotCount;
    private final int journalSlotSize;

    private CoreConfig(
            final int accountCapacity,
            final int dedupClientCapacity,
            final int dedupWindow,
            final int orderPoolCapacity,
            final int priceBucketCapacity,
            final int l2MaxLevels,
            final int eventBufferCapacity,
            final int journalSlotCount,
            final int journalSlotSize) {
        requirePositive("accountCapacity", accountCapacity);
        requirePositive("dedupClientCapacity", dedupClientCapacity);
        requirePowerOfTwo("dedupWindow", dedupWindow);
        requirePositive("orderPoolCapacity", orderPoolCapacity);
        requirePositive("priceBucketCapacity", priceBucketCapacity);
        requirePositive("l2MaxLevels", l2MaxLevels);
        requirePositive("eventBufferCapacity", eventBufferCapacity);
        requirePowerOfTwo("journalSlotCount", journalSlotCount);
        if (journalSlotSize <= 4) {
            // A slot must hold at least the length header plus one byte.
            throw new IllegalArgumentException("journalSlotSize must exceed 4, was: " + journalSlotSize);
        }
        this.accountCapacity = accountCapacity;
        this.dedupClientCapacity = dedupClientCapacity;
        this.dedupWindow = dedupWindow;
        this.orderPoolCapacity = orderPoolCapacity;
        this.priceBucketCapacity = priceBucketCapacity;
        this.l2MaxLevels = l2MaxLevels;
        this.eventBufferCapacity = eventBufferCapacity;
        this.journalSlotCount = journalSlotCount;
        this.journalSlotSize = journalSlotSize;
    }

    private static void requirePositive(final String name, final int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive, was: " + value);
        }
    }

    private static void requirePowerOfTwo(final String name, final int value) {
        if (value <= 0 || Integer.bitCount(value) != 1) {
            throw new IllegalArgumentException(name + " must be a power of two, was: " + value);
        }
    }

    public static CoreConfig defaults() {
        return new CoreConfig(
                DEFAULT_ACCOUNT_CAPACITY,
                DEFAULT_DEDUP_CLIENT_CAPACITY,
                DEFAULT_DEDUP_WINDOW,
                DEFAULT_ORDER_POOL_CAPACITY,
                DEFAULT_PRICE_BUCKET_CAPACITY,
                DEFAULT_L2_MAX_LEVELS,
                DEFAULT_EVENT_BUFFER_CAPACITY,
                DEFAULT_JOURNAL_SLOT_COUNT,
                DEFAULT_JOURNAL_SLOT_SIZE);
    }

    /**
     * Builds a configuration from {@code justrade.core.*} properties. Any key that is
     * absent or blank falls back to its default, so a partial override file is
     * valid; validation is applied once at {@link Builder#build()}.
     */
    public static CoreConfig fromProperties(final Properties props) {
        return builder()
                .accountCapacity(intProp(props, "accountCapacity", DEFAULT_ACCOUNT_CAPACITY))
                .dedupClientCapacity(intProp(props, "dedupClientCapacity", DEFAULT_DEDUP_CLIENT_CAPACITY))
                .dedupWindow(intProp(props, "dedupWindow", DEFAULT_DEDUP_WINDOW))
                .orderPoolCapacity(intProp(props, "orderPoolCapacity", DEFAULT_ORDER_POOL_CAPACITY))
                .priceBucketCapacity(intProp(props, "priceBucketCapacity", DEFAULT_PRICE_BUCKET_CAPACITY))
                .l2MaxLevels(intProp(props, "l2MaxLevels", DEFAULT_L2_MAX_LEVELS))
                .eventBufferCapacity(intProp(props, "eventBufferCapacity", DEFAULT_EVENT_BUFFER_CAPACITY))
                .journalSlotCount(intProp(props, "journalSlotCount", DEFAULT_JOURNAL_SLOT_COUNT))
                .journalSlotSize(intProp(props, "journalSlotSize", DEFAULT_JOURNAL_SLOT_SIZE))
                .build();
    }

    /** Builds a configuration from {@code -Djustrade.core.*} system properties, falling back to defaults. */
    public static CoreConfig fromSystemProperties() {
        return fromProperties(System.getProperties());
    }

    private static int intProp(final Properties props, final String key, final int defaultValue) {
        final String raw = props.getProperty(PROPERTY_PREFIX + key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("invalid integer for " + PROPERTY_PREFIX + key + ": " + raw);
        }
    }

    /** Starts a validated configuration from non-default capacities. */
    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder with validation at {@link #build()}. */
    public static final class Builder {
        private int accountCapacity = DEFAULT_ACCOUNT_CAPACITY;
        private int dedupClientCapacity = DEFAULT_DEDUP_CLIENT_CAPACITY;
        private int dedupWindow = DEFAULT_DEDUP_WINDOW;
        private int orderPoolCapacity = DEFAULT_ORDER_POOL_CAPACITY;
        private int priceBucketCapacity = DEFAULT_PRICE_BUCKET_CAPACITY;
        private int l2MaxLevels = DEFAULT_L2_MAX_LEVELS;
        private int eventBufferCapacity = DEFAULT_EVENT_BUFFER_CAPACITY;
        private int journalSlotCount = DEFAULT_JOURNAL_SLOT_COUNT;
        private int journalSlotSize = DEFAULT_JOURNAL_SLOT_SIZE;

        public Builder accountCapacity(final int value) {
            this.accountCapacity = value;
            return this;
        }

        public Builder dedupClientCapacity(final int value) {
            this.dedupClientCapacity = value;
            return this;
        }

        public Builder dedupWindow(final int value) {
            this.dedupWindow = value;
            return this;
        }

        public Builder orderPoolCapacity(final int value) {
            this.orderPoolCapacity = value;
            return this;
        }

        public Builder priceBucketCapacity(final int value) {
            this.priceBucketCapacity = value;
            return this;
        }

        public Builder l2MaxLevels(final int value) {
            this.l2MaxLevels = value;
            return this;
        }

        public Builder eventBufferCapacity(final int value) {
            this.eventBufferCapacity = value;
            return this;
        }

        public Builder journalSlotCount(final int value) {
            this.journalSlotCount = value;
            return this;
        }

        public Builder journalSlotSize(final int value) {
            this.journalSlotSize = value;
            return this;
        }

        public CoreConfig build() {
            return new CoreConfig(
                    accountCapacity,
                    dedupClientCapacity,
                    dedupWindow,
                    orderPoolCapacity,
                    priceBucketCapacity,
                    l2MaxLevels,
                    eventBufferCapacity,
                    journalSlotCount,
                    journalSlotSize);
        }
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

    public int priceBucketCapacity() {
        return priceBucketCapacity;
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
