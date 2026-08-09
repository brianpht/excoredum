package com.exadbe.xcorebench;

import java.util.List;

/** Formats side-by-side comparison tables for the console. */
public final class ComparisonReport {

    private final StringBuilder out = new StringBuilder();

    public ComparisonReport heading(final String title) {
        out.append(System.lineSeparator()).append(title).append(System.lineSeparator());
        out.append("=".repeat(title.length())).append(System.lineSeparator());
        return this;
    }

    public ComparisonReport note(final String line) {
        out.append(line).append(System.lineSeparator());
        return this;
    }

    public ComparisonReport table(final List<String> header, final List<List<String>> rows) {
        final int cols = header.size();
        final int[] width = new int[cols];
        for (int c = 0; c < cols; c++) {
            width[c] = header.get(c).length();
        }
        for (final List<String> row : rows) {
            for (int c = 0; c < cols; c++) {
                width[c] = Math.max(width[c], row.get(c).length());
            }
        }
        out.append(System.lineSeparator());
        appendRow(header, width);
        out.append("-".repeat(totalWidth(width) + 3)).append(System.lineSeparator());
        for (final List<String> row : rows) {
            appendRow(row, width);
        }
        return this;
    }

    public ComparisonReport mismatches(final String impl, final List<String> diff) {
        out.append(System.lineSeparator())
                .append("CROSS-VALIDATION FAILED for ")
                .append(impl)
                .append(':');
        for (final String d : diff) {
            out.append(System.lineSeparator()).append("  - ").append(d);
        }
        out.append(System.lineSeparator());
        return this;
    }

    private void appendRow(final List<String> row, final int[] width) {
        out.append(' ');
        for (int c = 0; c < width.length; c++) {
            out.append(pad(row.get(c), width[c])).append("   ");
        }
        out.append(System.lineSeparator());
    }

    private static int totalWidth(final int[] width) {
        int sum = 0;
        for (final int w : width) {
            sum += w + 3;
        }
        return sum;
    }

    /** Right-pads values, but right-aligns cells that look numeric. */
    private static String pad(final String value, final int width) {
        final boolean numeric = !value.isEmpty() && (Character.isDigit(value.charAt(0)) || value.charAt(0) == '-');
        if (numeric) {
            return " ".repeat(Math.max(0, width - value.length())) + value;
        }
        return value + " ".repeat(Math.max(0, width - value.length()));
    }

    public String render() {
        return out.toString();
    }

    public static String mts(final long commands, final long nanos) {
        return String.format("%.3f", commands / 1_000_000.0 / (nanos / 1_000_000_000.0));
    }

    public static String micros(final long nanos) {
        return String.format("%.1f", nanos / 1000.0);
    }
}
