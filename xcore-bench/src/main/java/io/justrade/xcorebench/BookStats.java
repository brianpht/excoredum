package io.justrade.xcorebench;

import java.util.ArrayList;
import java.util.List;

/**
 * Outcome of one workload replay through an order book: event counters plus a
 * full-depth L2 digest. Two engines replaying the same workload must produce
 * identical stats; {@link #diff} reports the first mismatch.
 */
public record BookStats(
        String name,
        long replayNanos,
        long trades,
        long tradeVolume,
        long rejects,
        long rejectedSize,
        long reduces,
        long reducedSize,
        int askLevels,
        int bidLevels,
        long askVolumeTotal,
        long bidVolumeTotal,
        long l2Checksum) {

    /** Returns a list of mismatch descriptions; empty when both replays agree. */
    public static List<String> diff(final BookStats expected, final BookStats actual) {
        final List<String> mismatches = new ArrayList<>();
        check(mismatches, "trades", expected.trades, actual.trades);
        check(mismatches, "tradeVolume", expected.tradeVolume, actual.tradeVolume);
        check(mismatches, "rejects", expected.rejects, actual.rejects);
        check(mismatches, "rejectedSize", expected.rejectedSize, actual.rejectedSize);
        check(mismatches, "reduces", expected.reduces, actual.reduces);
        check(mismatches, "reducedSize", expected.reducedSize, actual.reducedSize);
        check(mismatches, "askLevels", expected.askLevels, actual.askLevels);
        check(mismatches, "bidLevels", expected.bidLevels, actual.bidLevels);
        check(mismatches, "askVolumeTotal", expected.askVolumeTotal, actual.askVolumeTotal);
        check(mismatches, "bidVolumeTotal", expected.bidVolumeTotal, actual.bidVolumeTotal);
        check(mismatches, "l2Checksum", expected.l2Checksum, actual.l2Checksum);
        return mismatches;
    }

    private static void check(final List<String> out, final String field, final long expected, final long actual) {
        if (expected != actual) {
            out.add(field + ": expected=" + expected + " actual=" + actual);
        }
    }

    /** FNV-1a style mix of one (price, volume) level pair into a running hash. */
    public static long mixLevel(final long hash, final long price, final long volume) {
        long h = hash ^ (price * 0x9E3779B97F4A7C15L);
        h *= 0x100000001B3L;
        h ^= volume * 0xC2B2AE3D27D4EB4FL;
        h *= 0x100000001b3L;
        return h;
    }
}
