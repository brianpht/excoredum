package com.exadbe.gateway.codec;

/**
 * Fixed-scale decimal to long conversion for the REST boundary. Money stays
 * 64-bit integer with a fixed scale everywhere inside the gateway and the
 * core; these routines translate human-readable decimals at the edge without
 * BigDecimal.
 */
public final class DecimalCodec {

    /** Parse result: input converted successfully. */
    public static final int OK = 0;

    /** Parse result: input is not a valid decimal number. */
    public static final int MALFORMED = 1;

    /** Parse result: input has significant fractional digits beyond the scale. */
    public static final int PRECISION_TOO_HIGH = 2;

    /** Parse result: converted magnitude does not fit a signed 64-bit long. */
    public static final int OVERFLOW = 3;

    /** Powers of ten, {@code POW10[i] = 10^i}, for digit emission. */
    static final long[] POW10 = {
        1L,
        10L,
        100L,
        1_000L,
        10_000L,
        100_000L,
        1_000_000L,
        10_000_000L,
        100_000_000L,
        1_000_000_000L,
        10_000_000_000L,
        100_000_000_000L,
        1_000_000_000_000L,
        10_000_000_000_000L,
        100_000_000_000_000L,
        1_000_000_000_000_000L,
        10_000_000_000_000_000L,
        100_000_000_000_000_000L,
        1_000_000_000_000_000_000L,
    };

    private DecimalCodec() {}

    /**
     * Parses a decimal string into a long scaled by {@code 10^scale}. Accepts
     * an optional sign, integer digits, and an optional fraction with at most
     * {@code scale} significant digits (trailing zeros beyond the scale are
     * allowed). The symmetric range {@code [-Long.MAX_VALUE, Long.MAX_VALUE]}
     * is supported.
     *
     * @param text decimal input, e.g. {@code "123.45"}
     * @param scale number of implied decimal places of the output unit
     * @param out single-element holder receiving the scaled value on OK
     * @return one of OK, MALFORMED, PRECISION_TOO_HIGH, OVERFLOW
     */
    public static int parseScaled(final CharSequence text, final int scale, final long[] out) {
        out[0] = 0L;
        final int length = text == null ? 0 : text.length();
        if (length == 0 || scale < 0) {
            return MALFORMED;
        }
        int i = 0;
        final char first = text.charAt(0);
        boolean negative = false;
        if (first == '-') {
            negative = true;
            i = 1;
        } else if (first == '+') {
            i = 1;
        }
        if (i == length) {
            return MALFORMED;
        }
        long value = 0L;
        boolean anyDigit = false;
        while (i < length) {
            final char c = text.charAt(i);
            if (c == '.') {
                i++;
                break;
            }
            if (c < '0' || c > '9') {
                return MALFORMED;
            }
            final int digit = c - '0';
            if (value > (Long.MAX_VALUE - digit) / 10L) {
                return OVERFLOW;
            }
            value = (value * 10L) + digit;
            anyDigit = true;
            i++;
        }
        int fractionDigits = 0;
        while (i < length) {
            final char c = text.charAt(i++);
            if (c < '0' || c > '9') {
                return MALFORMED;
            }
            final int digit = c - '0';
            anyDigit = true;
            if (fractionDigits < scale) {
                if (value > (Long.MAX_VALUE - digit) / 10L) {
                    return OVERFLOW;
                }
                value = (value * 10L) + digit;
                fractionDigits++;
            } else if (digit != 0) {
                return PRECISION_TOO_HIGH;
            }
        }
        if (!anyDigit) {
            return MALFORMED;
        }
        for (int k = fractionDigits; k < scale; k++) {
            if (value > Long.MAX_VALUE / 10L) {
                return OVERFLOW;
            }
            value *= 10L;
        }
        out[0] = negative ? -value : value;
        return OK;
    }

    /**
     * Appends the decimal representation of a scaled long to {@code out} as a
     * raw JSON number: {@code value=12345, scale=2} renders {@code 123.45};
     * {@code value=5, scale=3} renders {@code 0.005}. Allocates nothing.
     *
     * @param value the scaled value
     * @param scale number of implied decimal places
     * @param out writer receiving the digits
     */
    public static void formatScaled(final long value, final int scale, final JsonWriter out) {
        if (value < 0L) {
            out.appendAscii('-');
        }
        if (value == Long.MIN_VALUE) {
            // -Long.MIN_VALUE overflows; emit the known magnitude directly.
            emitDigitString("9223372036854775808", scale, out);
            return;
        }
        final long magnitude = value < 0L ? -value : value;
        int total = 1;
        for (long m = magnitude; m >= 10L; m /= 10L) {
            total++;
        }
        long divisor = POW10[total - 1];
        long remaining = magnitude;
        final int integerDigits = total - scale;
        if (integerDigits <= 0) {
            out.appendAscii('0');
            if (scale > 0) {
                out.appendAscii('.');
                for (int z = integerDigits; z < 0; z++) {
                    out.appendAscii('0');
                }
                emitRemaining(remaining, divisor, total, out);
            }
            return;
        }
        for (int i = 0; i < integerDigits; i++) {
            out.appendAscii((char) ('0' + (remaining / divisor)));
            remaining %= divisor;
            divisor /= 10L;
        }
        if (scale > 0) {
            out.appendAscii('.');
            emitRemaining(remaining, divisor, scale, out);
        }
    }

    /** Emits {@code count} fraction digits from {@code remaining}, zero-padded. */
    private static void emitRemaining(long remaining, long divisor, final int count, final JsonWriter out) {
        for (int i = 0; i < count; i++) {
            if (divisor > 0L) {
                out.appendAscii((char) ('0' + (remaining / divisor)));
                remaining %= divisor;
                divisor /= 10L;
            } else {
                out.appendAscii('0');
            }
        }
    }

    /** Emits a decimal point into a digit string of length {@code digits.length()}. */
    private static void emitDigitString(final String digits, final int scale, final JsonWriter out) {
        final int total = digits.length();
        final int integerDigits = total - scale;
        int i = 0;
        if (integerDigits <= 0) {
            out.appendAscii('0');
            if (scale > 0) {
                out.appendAscii('.');
                for (int z = integerDigits; z < 0; z++) {
                    out.appendAscii('0');
                }
                while (i < total) {
                    out.appendAscii(digits.charAt(i++));
                }
            }
            return;
        }
        while (i < integerDigits) {
            out.appendAscii(digits.charAt(i++));
        }
        if (scale > 0) {
            out.appendAscii('.');
            while (i < total) {
                out.appendAscii(digits.charAt(i++));
            }
        }
    }
}
