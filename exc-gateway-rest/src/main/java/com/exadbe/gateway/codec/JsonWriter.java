package com.exadbe.gateway.codec;

import java.util.Arrays;

/**
 * Minimal reusable JSON writer for gateway responses. Writes UTF-8 JSON into a
 * preallocated, grow-only byte buffer; one instance is owned by the gateway
 * agent thread and reset per response, so steady-state serialization performs
 * no allocation until a response exceeds the high-water buffer size.
 */
public final class JsonWriter {

    private static final int MAX_DEPTH = 8;

    private byte[] buffer;
    private int length;
    private int depth;
    private boolean expectValue;
    private final boolean[] first = new boolean[MAX_DEPTH];

    public JsonWriter(final int initialCapacity) {
        this.buffer = new byte[Math.max(64, initialCapacity)];
    }

    /** Clears all content and nesting state; the buffer is retained for reuse. */
    public void reset() {
        length = 0;
        depth = 0;
        expectValue = false;
    }

    public JsonWriter beginObject() {
        separator();
        ensure(1);
        buffer[length++] = '{';
        push();
        return this;
    }

    public JsonWriter endObject() {
        ensure(1);
        buffer[length++] = '}';
        depth--;
        return this;
    }

    public JsonWriter beginArray() {
        separator();
        ensure(1);
        buffer[length++] = '[';
        push();
        return this;
    }

    public JsonWriter endArray() {
        ensure(1);
        buffer[length++] = ']';
        depth--;
        return this;
    }

    /** Writes a member name; the next value call emits its value without a comma. */
    public JsonWriter name(final String key) {
        separator();
        writeQuoted(key);
        ensure(1);
        buffer[length++] = ':';
        expectValue = true;
        return this;
    }

    public JsonWriter valueLong(final long value) {
        separator();
        writeLong(value);
        return this;
    }

    public JsonWriter valueString(final String value) {
        separator();
        if (value == null) {
            writeNull();
        } else {
            writeQuoted(value);
        }
        return this;
    }

    public JsonWriter valueBoolean(final boolean value) {
        separator();
        writeAscii(value ? "true" : "false");
        return this;
    }

    public JsonWriter valueNull() {
        separator();
        writeNull();
        return this;
    }

    /** Writes a scaled long as a decimal JSON number via {@link DecimalCodec}. */
    public JsonWriter valueDecimal(final long value, final int scale) {
        separator();
        DecimalCodec.formatScaled(value, scale, this);
        return this;
    }

    /** Low-level: appends one raw ASCII byte with no JSON framing. */
    public void appendAscii(final char c) {
        ensure(1);
        buffer[length++] = (byte) c;
    }

    /** Low-level: appends raw ASCII bytes with no JSON framing. */
    public void appendAscii(final String s) {
        writeAscii(s);
    }

    /** The backing buffer, valid up to {@link #length()}. */
    public byte[] buffer() {
        return buffer;
    }

    public int length() {
        return length;
    }

    private void push() {
        depth++;
        first[depth] = true;
    }

    private void separator() {
        if (expectValue) {
            expectValue = false;
            return;
        }
        if (depth > 0) {
            if (!first[depth]) {
                ensure(1);
                buffer[length++] = ',';
            }
            first[depth] = false;
        }
    }

    private void writeLong(long value) {
        if (value == Long.MIN_VALUE) {
            writeAscii("-9223372036854775808");
            return;
        }
        if (value < 0L) {
            ensure(1);
            buffer[length++] = '-';
            value = -value;
        }
        int total = 1;
        for (long m = value; m >= 10L; m /= 10L) {
            total++;
        }
        ensure(total);
        long divisor = DecimalCodec.POW10[total - 1];
        long remaining = value;
        for (int i = 0; i < total; i++) {
            buffer[length++] = (byte) ('0' + (remaining / divisor));
            remaining %= divisor;
            divisor /= 10L;
        }
    }

    private void writeNull() {
        writeAscii("null");
    }

    private void writeAscii(final String s) {
        ensure(s.length());
        for (int i = 0; i < s.length(); i++) {
            buffer[length++] = (byte) s.charAt(i);
        }
    }

    private void writeQuoted(final String s) {
        ensure(s.length() + 2);
        buffer[length++] = '"';
        for (int i = 0; i < s.length(); i++) {
            final char c = s.charAt(i);
            if (c == '"' || c == '\\') {
                ensure(2);
                buffer[length++] = '\\';
                buffer[length++] = (byte) c;
            } else if (c < 0x20) {
                ensure(6);
                buffer[length++] = '\\';
                buffer[length++] = 'u';
                buffer[length++] = '0';
                buffer[length++] = '0';
                buffer[length++] = (byte) hex((c >> 4) & 0xF);
                buffer[length++] = (byte) hex(c & 0xF);
            } else if (c < 0x80) {
                ensure(1);
                buffer[length++] = (byte) c;
            } else if (c < 0x800) {
                ensure(2);
                buffer[length++] = (byte) (0xC0 | (c >> 6));
                buffer[length++] = (byte) (0x80 | (c & 0x3F));
            } else {
                ensure(3);
                buffer[length++] = (byte) (0xE0 | (c >> 12));
                buffer[length++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                buffer[length++] = (byte) (0x80 | (c & 0x3F));
            }
        }
        ensure(1);
        buffer[length++] = '"';
    }

    private static char hex(final int nibble) {
        return (char) (nibble < 10 ? '0' + nibble : 'a' + nibble - 10);
    }

    private void ensure(final int extra) {
        if (length + extra <= buffer.length) {
            return;
        }
        int newCapacity = buffer.length;
        while (length + extra > newCapacity) {
            newCapacity <<= 1;
        }
        buffer = Arrays.copyOf(buffer, newCapacity);
    }
}
