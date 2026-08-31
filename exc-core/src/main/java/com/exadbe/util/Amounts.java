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

    /**
     * Returns {@code true} if {@code a * b} overflows a signed 64-bit long.
     * Branch-light: compares the high 64 bits of the 128-bit product against the
     * sign extension of the low 64 bits (exact for every operand combination,
     * including {@code Long.MIN_VALUE * -1}).
     */
    public static boolean mulOverflows(final long a, final long b) {
        final long lo = a * b;
        return Math.multiplyHigh(a, b) != (lo >> 63);
    }
}
