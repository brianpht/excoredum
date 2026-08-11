package com.exadbe.gateway.codec;

import java.nio.charset.StandardCharsets;

/**
 * Minimal pull-style JSON object reader for gateway request bodies. Bodies are
 * flat JSON objects with scalar values; unknown nested objects and arrays are
 * skipped. Parse failures set a sticky {@link #failed()} flag and all
 * accessors degrade to sentinels, so callers validate once after parsing
 * instead of branching per field. Values are expected to be ASCII; non-ASCII
 * bytes pass through as ISO-8859-1 characters.
 */
public final class JsonReader {

    private static final String EMPTY = "";

    private byte[] buffer = new byte[0];
    private int pos;
    private int end;
    private boolean failed;
    private boolean firstField;

    /** Points the reader at a body; resets all parse state. */
    public void wrap(final byte[] data, final int length) {
        this.buffer = data;
        this.pos = 0;
        this.end = length;
        this.failed = false;
        this.firstField = true;
    }

    /** Whether any parse error has been encountered since the last wrap. */
    public boolean failed() {
        return failed;
    }

    /** Consumes the opening brace of the top-level object. */
    public boolean beginObject() {
        if (failed) {
            return false;
        }
        skipWhitespace();
        if (pos >= end || buffer[pos++] != '{') {
            failed = true;
            return false;
        }
        firstField = true;
        return true;
    }

    /**
     * Advances to the next field; consumes the closing brace and returns
     * {@code false} at the object end. Call once per field, before
     * {@link #fieldName()}.
     */
    public boolean hasNextField() {
        if (failed) {
            return false;
        }
        skipWhitespace();
        if (pos >= end) {
            failed = true;
            return false;
        }
        final byte c = buffer[pos];
        if (c == '}') {
            pos++;
            return false;
        }
        if (!firstField) {
            if (c != ',') {
                failed = true;
                return false;
            }
            pos++;
            skipWhitespace();
        }
        firstField = false;
        return true;
    }

    /** Reads the current field name and the following colon. */
    public String fieldName() {
        if (failed) {
            return EMPTY;
        }
        final String name = readString();
        if (failed) {
            return EMPTY;
        }
        skipWhitespace();
        if (pos >= end || buffer[pos++] != ':') {
            failed = true;
            return EMPTY;
        }
        return name;
    }

    /** Reads an unquoted JSON number as a long; sets {@link #failed()} when malformed or overflowing. */
    public long nextLong() {
        if (failed) {
            return 0L;
        }
        skipWhitespace();
        if (pos >= end) {
            failed = true;
            return 0L;
        }
        boolean negative = false;
        byte c = buffer[pos];
        if (c == '-') {
            negative = true;
            pos++;
        } else if (c == '+') {
            pos++;
        }
        long value = 0L;
        boolean anyDigit = false;
        while (pos < end) {
            c = buffer[pos];
            if (c < '0' || c > '9') {
                break;
            }
            pos++;
            final int digit = c - '0';
            if (value > (Long.MAX_VALUE - digit) / 10L) {
                failed = true;
                return 0L;
            }
            value = (value * 10L) + digit;
            anyDigit = true;
        }
        if (!anyDigit) {
            failed = true;
            return 0L;
        }
        return negative ? -value : value;
    }

    /** Reads a quoted JSON string value. */
    public String nextString() {
        if (failed) {
            return EMPTY;
        }
        return readString();
    }

    /**
     * Reads a scalar value as raw text for decimal conversion: quoted strings
     * are unquoted, bare numbers are captured verbatim. Accepts both
     * {@code "123.45"} and {@code 123.45}.
     */
    public String nextToken() {
        if (failed) {
            return EMPTY;
        }
        skipWhitespace();
        if (pos >= end) {
            failed = true;
            return EMPTY;
        }
        if (buffer[pos] == '"') {
            return readString();
        }
        final int start = pos;
        while (pos < end) {
            final byte b = buffer[pos];
            if (b == ',' || b == '}' || b == ']' || b == ' ' || b == '\t' || b == '\n' || b == '\r') {
                break;
            }
            pos++;
        }
        if (pos == start) {
            failed = true;
            return EMPTY;
        }
        return new String(buffer, start, pos - start, StandardCharsets.US_ASCII);
    }

    /** Skips one value of any type (scalar, nested object, or array). */
    public void skipValue() {
        if (failed) {
            return;
        }
        skipWhitespace();
        if (pos >= end) {
            failed = true;
            return;
        }
        final byte c = buffer[pos];
        if (c == '"') {
            readString();
            return;
        }
        if (c == '{' || c == '[') {
            final byte open = c;
            final byte close = c == '{' ? (byte) '}' : (byte) ']';
            int nesting = 0;
            boolean inString = false;
            while (pos < end) {
                final byte b = buffer[pos++];
                if (inString) {
                    if (b == '\\') {
                        if (pos < end) {
                            pos++;
                        }
                    } else if (b == '"') {
                        inString = false;
                    }
                } else if (b == '"') {
                    inString = true;
                } else if (b == open) {
                    nesting++;
                } else if (b == close) {
                    nesting--;
                    if (nesting == 0) {
                        return;
                    }
                }
            }
            failed = true;
            return;
        }
        while (pos < end) {
            final byte b = buffer[pos];
            if (b == ',' || b == '}' || b == ']' || b == ' ' || b == '\t' || b == '\n' || b == '\r') {
                return;
            }
            pos++;
        }
    }

    private String readString() {
        skipWhitespace();
        if (pos >= end || buffer[pos++] != '"') {
            failed = true;
            return EMPTY;
        }
        final StringBuilder sb = new StringBuilder(16);
        while (pos < end) {
            final byte b = buffer[pos++];
            if (b == '"') {
                return sb.toString();
            }
            if (b == '\\') {
                if (!readEscape(sb)) {
                    return EMPTY;
                }
            } else if ((b & 0xFF) < 0x20) {
                failed = true;
                return EMPTY;
            } else {
                sb.append((char) (b & 0xFF));
            }
        }
        failed = true;
        return EMPTY;
    }

    private boolean readEscape(final StringBuilder sb) {
        if (pos >= end) {
            failed = true;
            return false;
        }
        final byte esc = buffer[pos++];
        switch (esc) {
            case '"':
                sb.append('"');
                break;
            case '\\':
                sb.append('\\');
                break;
            case '/':
                sb.append('/');
                break;
            case 'b':
                sb.append('\b');
                break;
            case 'f':
                sb.append('\f');
                break;
            case 'n':
                sb.append('\n');
                break;
            case 'r':
                sb.append('\r');
                break;
            case 't':
                sb.append('\t');
                break;
            case 'u':
                if (pos + 4 > end) {
                    failed = true;
                    return false;
                }
                int codepoint = 0;
                for (int i = 0; i < 4; i++) {
                    final int digit = hexDigit(buffer[pos++]);
                    if (digit < 0) {
                        failed = true;
                        return false;
                    }
                    codepoint = (codepoint << 4) | digit;
                }
                sb.append(codepoint >= 0x20 && codepoint < 0x7F ? (char) codepoint : '?');
                break;
            default:
                failed = true;
                return false;
        }
        return true;
    }

    private void skipWhitespace() {
        while (pos < end) {
            final byte b = buffer[pos];
            if (b != ' ' && b != '\t' && b != '\n' && b != '\r') {
                return;
            }
            pos++;
        }
    }

    private static int hexDigit(final byte b) {
        if (b >= '0' && b <= '9') {
            return b - '0';
        }
        if (b >= 'a' && b <= 'f') {
            return b - 'a' + 10;
        }
        if (b >= 'A' && b <= 'F') {
            return b - 'A' + 10;
        }
        return -1;
    }
}
