package com.exadbe.util;

/**
 * Branch-light, allocation-free integer money arithmetic. Overflow is detected
 * and reported to the caller as a boolean rather than thrown, so the hot path
 * never uses exceptions for control flow.
 */
public final class Amounts {

    private Amounts() {}

    /** Returns {@code true} if {@code a + b} overflows a signed 64-bit long. */
    public static boolean addOverflows(final long a, final long b) {
        final long r = a + b;
        // Overflow iff the operands share a sign and the result differs from it.
        return ((a ^ r) & (b ^ r)) < 0L;
    }

    /** Returns {@code true} if {@code amount} is strictly negative. */
    public static boolean isNegative(final long amount) {
        return amount < 0L;
    }
}
