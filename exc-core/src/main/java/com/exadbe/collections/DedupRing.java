package com.exadbe.collections;

import java.util.Arrays;

/**
 * Fixed-capacity, allocation-free ring holding the most recent command results
 * for one client, indexed directly by {@code clientSeq & (capacity - 1)}.
 *
 * <p>Because {@code clientSeq} is monotonic per client, the slot for a sequence
 * is deterministic and lookup is O(1): the slot is occupied by that sequence iff
 * {@code seqSlots[seq & mask] == seq}. Retries reuse the same {@code clientSeq},
 * so they resolve to the same slot and are detected as duplicates.
 *
 * <p>Contract: {@code clientSeq} must never equal {@link #EMPTY} (all-ones),
 * which is reserved as the unoccupied-slot sentinel.
 */
public final class DedupRing {

    /** Reserved sentinel marking an unoccupied slot. */
    public static final long EMPTY = -1L;

    /** Flag bit indicating the cached result carried a uid. */
    public static final int FLAG_HAS_UID = 1;

    /** Flag bit indicating the cached result carried an order id. */
    public static final int FLAG_HAS_ORDER_ID = 1 << 1;

    /** Flag bit indicating the cached result carried a filled size. */
    public static final int FLAG_HAS_FILLED_SIZE = 1 << 2;

    private final int mask;
    private final long[] seqSlots;
    private final long[] commandIdHi;
    private final long[] commandIdLo;
    private final int[] resultCode;
    private final long[] uid;
    private final long[] orderId;
    private final long[] filledSize;
    private final byte[] flags;

    public DedupRing(final int capacity) {
        if (capacity <= 0 || Integer.bitCount(capacity) != 1) {
            // Ring indexing uses `seq & (capacity - 1)`, which requires a power of
            // two; a non-power-of-two capacity would alias slots and corrupt dedup.
            throw new IllegalArgumentException("dedup window must be a power of two, was: " + capacity);
        }
        this.mask = capacity - 1;
        this.seqSlots = new long[capacity];
        this.commandIdHi = new long[capacity];
        this.commandIdLo = new long[capacity];
        this.resultCode = new int[capacity];
        this.uid = new long[capacity];
        this.orderId = new long[capacity];
        this.filledSize = new long[capacity];
        this.flags = new byte[capacity];
        Arrays.fill(seqSlots, EMPTY);
    }

    /** Returns {@code true} if this sequence's slot currently holds that sequence. */
    public boolean contains(final long seq) {
        return seq != EMPTY && seqSlots[(int) (seq & mask)] == seq;
    }

    /**
     * Stores (or overwrites) the cached result for a sequence.
     *
     * @return {@code true} if storing evicted a different, older sequence.
     */
    public boolean put(
            final long seq,
            final long idHi,
            final long idLo,
            final int code,
            final long uidValue,
            final boolean hasUid,
            final long orderIdValue,
            final boolean hasOrderId,
            final long filledSizeValue,
            final boolean hasFilledSize) {
        if (seq == EMPTY) {
            // The sentinel is reserved for unoccupied slots; writing it would erase
            // whatever sequence shares this slot. The engine rejects such seqs.
            return false;
        }
        final int idx = (int) (seq & mask);
        final long prior = seqSlots[idx];
        final boolean evicted = prior != EMPTY && prior != seq;
        seqSlots[idx] = seq;
        commandIdHi[idx] = idHi;
        commandIdLo[idx] = idLo;
        resultCode[idx] = code;
        uid[idx] = uidValue;
        orderId[idx] = orderIdValue;
        filledSize[idx] = filledSizeValue;
        int f = 0;
        if (hasUid) {
            f |= FLAG_HAS_UID;
        }
        if (hasOrderId) {
            f |= FLAG_HAS_ORDER_ID;
        }
        if (hasFilledSize) {
            f |= FLAG_HAS_FILLED_SIZE;
        }
        flags[idx] = (byte) f;
        return evicted;
    }

    public long commandIdHi(final long seq) {
        return commandIdHi[(int) (seq & mask)];
    }

    public long commandIdLo(final long seq) {
        return commandIdLo[(int) (seq & mask)];
    }

    public int resultCode(final long seq) {
        return resultCode[(int) (seq & mask)];
    }

    public long uid(final long seq) {
        return uid[(int) (seq & mask)];
    }

    public long orderId(final long seq) {
        return orderId[(int) (seq & mask)];
    }

    public long filledSize(final long seq) {
        return filledSize[(int) (seq & mask)];
    }

    public boolean hasUid(final long seq) {
        return (flags[(int) (seq & mask)] & FLAG_HAS_UID) != 0;
    }

    public boolean hasOrderId(final long seq) {
        return (flags[(int) (seq & mask)] & FLAG_HAS_ORDER_ID) != 0;
    }

    public boolean hasFilledSize(final long seq) {
        return (flags[(int) (seq & mask)] & FLAG_HAS_FILLED_SIZE) != 0;
    }

    /** Ring capacity (power of two). */
    public int capacity() {
        return mask + 1;
    }

    /**
     * Copies every occupied sequence into {@code dest} and returns the count.
     *
     * <p>Cold snapshot path only; {@code dest} must be at least {@link #capacity()}.
     */
    public int occupiedSeqs(final long[] dest) {
        int n = 0;
        for (final long slot : seqSlots) {
            if (slot != EMPTY) {
                dest[n++] = slot;
            }
        }
        return n;
    }
}
