package com.exadbe.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.exadbe.gateway.codec.DecimalCodec;
import com.exadbe.gateway.codec.JsonWriter;
import org.junit.jupiter.api.Test;

/** Fixed-scale decimal conversion at the REST boundary: parse, reject, and format. */
class DecimalCodecTest {

    private final long[] out = new long[1];

    private int parse(final String text, final int scale) {
        return DecimalCodec.parseScaled(text, scale, out);
    }

    private String format(final long value, final int scale) {
        final JsonWriter writer = new JsonWriter(64);
        DecimalCodec.formatScaled(value, scale, writer);
        return new String(writer.buffer(), 0, writer.length(), java.nio.charset.StandardCharsets.US_ASCII);
    }

    @Test
    void parsesDecimalsAtScale() {
        assertEquals(DecimalCodec.OK, parse("123.45", 2));
        assertEquals(12345L, out[0]);

        assertEquals(DecimalCodec.OK, parse("100", 2));
        assertEquals(10000L, out[0]);

        assertEquals(DecimalCodec.OK, parse("0.005", 3));
        assertEquals(5L, out[0]);

        assertEquals(DecimalCodec.OK, parse("-1.5", 2));
        assertEquals(-150L, out[0]);

        assertEquals(DecimalCodec.OK, parse("+0.5", 2));
        assertEquals(50L, out[0]);

        assertEquals(DecimalCodec.OK, parse("1.500", 2));
        assertEquals(150L, out[0], "trailing zeros beyond the scale are allowed");

        assertEquals(DecimalCodec.OK, parse("0", 4));
        assertEquals(0L, out[0]);
    }

    @Test
    void rejectsMalformedInput() {
        assertEquals(DecimalCodec.MALFORMED, parse("", 2));
        assertEquals(DecimalCodec.MALFORMED, parse(null, 2));
        assertEquals(DecimalCodec.MALFORMED, parse(".", 2));
        assertEquals(DecimalCodec.MALFORMED, parse("-", 2));
        assertEquals(DecimalCodec.MALFORMED, parse("1..5", 2));
        assertEquals(DecimalCodec.MALFORMED, parse("1.5x", 2));
        assertEquals(DecimalCodec.MALFORMED, parse("abc", 2));
    }

    @Test
    void rejectsExcessPrecision() {
        assertEquals(DecimalCodec.PRECISION_TOO_HIGH, parse("1.505", 2));
        assertEquals(DecimalCodec.PRECISION_TOO_HIGH, parse("0.001", 2));
    }

    @Test
    void rejectsOverflow() {
        assertEquals(DecimalCodec.OVERFLOW, parse("9223372036854775808", 0));
        assertEquals(DecimalCodec.OVERFLOW, parse("922337203685477580.8", 1));
        assertEquals(DecimalCodec.PRECISION_TOO_HIGH, parse("92233720368547758.08", 1));
        assertEquals(DecimalCodec.OK, parse("9223372036854775807", 0));
        assertEquals(Long.MAX_VALUE, out[0]);
    }

    @Test
    void formatsScaledValues() {
        assertEquals("123.45", format(12345L, 2));
        assertEquals("-123.45", format(-12345L, 2));
        assertEquals("0.005", format(5L, 3));
        assertEquals("0.00", format(0L, 2));
        assertEquals("100", format(100L, 0));
        assertEquals("-0.5", format(-5L, 1));
        assertEquals("1000.00", format(100000L, 2));
    }

    @Test
    void formatsLongMinValue() {
        assertEquals("-92233720368547758.08", format(Long.MIN_VALUE, 2));
    }

    @Test
    void roundTrips() {
        final String[] samples = {"0.00", "1.00", "0.01", "123.45", "99999.99", "-42.70"};
        for (final String sample : samples) {
            assertEquals(DecimalCodec.OK, parse(sample, 2), sample);
            assertEquals(sample, format(out[0], 2), sample);
        }
    }
}
