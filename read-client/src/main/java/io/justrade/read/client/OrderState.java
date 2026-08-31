package io.justrade.read.client;

/**
 * Order lifecycle states as mirrored from the read replica's {@code OrderRecord};
 * the numeric values match the replica's constants so the wire state passes
 * through unchanged.
 */
public final class OrderState {

    /** Placement applied, result not yet seen (never survives one command's outcome). */
    public static final int NEW = 0;

    /** Resting after a successful place. */
    public static final int ACTIVE = 1;

    /** Fully cancelled or reduced. */
    public static final int CANCELLED = 2;

    /** Fully filled. */
    public static final int COMPLETED = 3;

    /** Placement rejected, or unmatched remainder rejected (IOC / FOK). */
    public static final int REJECTED = 4;

    /** The name of a state constant. */
    public static String name(final int state) {
        switch (state) {
            case NEW:
                return "NEW";
            case ACTIVE:
                return "ACTIVE";
            case CANCELLED:
                return "CANCELLED";
            case COMPLETED:
                return "COMPLETED";
            case REJECTED:
                return "REJECTED";
            default:
                return "UNKNOWN";
        }
    }

    private OrderState() {}
}
