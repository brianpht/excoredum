package com.exadbe.collections;

import java.util.Arrays;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * Single-writer store of spot account balances, keyed by the composite
 * {@code (uid, currency)}. Each user owns a {@link Long2LongHashMap} of
 * per-currency balances (currency ids widened to long keys), created when the
 * user is added.
 *
 * <p>{@link Long2ObjectHashMap} is used only for O(1) key lookup, never for
 * order-dependent iteration at runtime. Deterministic ordering is imposed
 * explicitly in {@link #forEachSorted(BalanceConsumer)} for snapshots (phase 4).
 */
public final class AccountStore {

    /** Sentinel returned when a currency balance is absent for a user. */
    public static final long MISSING = Long.MIN_VALUE;

    /** An active user may place orders. */
    public static final int STATUS_ACTIVE = 0;

    /** A suspended user is blocked from placing new orders. */
    public static final int STATUS_SUSPENDED = 1;

    // Distinct from any real status so STATUS_ACTIVE (0) can be stored explicitly.
    private static final long MISSING_STATUS = Long.MIN_VALUE;

    private static final float LOAD_FACTOR = 0.65f;
    private static final int CURRENCY_CAPACITY = 8;

    private final Long2ObjectHashMap<Long2LongHashMap> byUser;

    // uid -> status; an absent key is treated as STATUS_ACTIVE.
    private final Long2LongHashMap statusByUser;

    private long[] userScratch = new long[0];
    private long[] currencyScratch = new long[0];

    public AccountStore(final int userCapacity) {
        this.byUser = new Long2ObjectHashMap<>(userCapacity, LOAD_FACTOR);
        this.statusByUser = new Long2LongHashMap(userCapacity, LOAD_FACTOR, MISSING_STATUS);
    }

    /** Returns {@code true} if the user exists. */
    public boolean userExists(final long uid) {
        return byUser.containsKey(uid);
    }

    /**
     * Creates an account for {@code uid}.
     *
     * @return {@code true} if created, {@code false} if the user already exists.
     */
    public boolean addUser(final long uid) {
        if (byUser.containsKey(uid)) {
            return false;
        }
        byUser.put(uid, new Long2LongHashMap(CURRENCY_CAPACITY, LOAD_FACTOR, MISSING));
        return true;
    }

    /** Returns the balance for {@code (uid, currency)}; zero if the currency is unseen. */
    public long balance(final long uid, final int currency) {
        final Long2LongHashMap balances = byUser.get(uid);
        if (balances == null) {
            return 0L;
        }
        final long value = balances.get(currency);
        return value == MISSING ? 0L : value;
    }

    /** Sets the balance for {@code (uid, currency)}; the user must exist. */
    public void set(final long uid, final int currency, final long value) {
        byUser.get(uid).put(currency, value);
    }

    /** Adds {@code delta} to {@code (uid, currency)} and returns the new balance; user must exist. */
    public long addToValue(final long uid, final int currency, final long delta) {
        final Long2LongHashMap balances = byUser.get(uid);
        final long cur = balances.get(currency);
        final long base = cur == MISSING ? 0L : cur;
        final long updated = base + delta;
        balances.put(currency, updated);
        return updated;
    }

    /** Number of users. */
    public int userCount() {
        return byUser.size();
    }

    /** Returns the user's status ({@link #STATUS_ACTIVE} or {@link #STATUS_SUSPENDED}). */
    public int status(final long uid) {
        final long value = statusByUser.get(uid);
        return value == MISSING_STATUS ? STATUS_ACTIVE : (int) value;
    }

    /** Returns {@code true} if the user is suspended. */
    public boolean isSuspended(final long uid) {
        return statusByUser.get(uid) == STATUS_SUSPENDED;
    }

    /** Sets the user's status; the user must exist. */
    public void setStatus(final long uid, final int status) {
        statusByUser.put(uid, status);
    }

    /** Removes all state; used before a snapshot load. */
    public void clear() {
        byUser.clear();
        statusByUser.clear();
    }

    /**
     * Emits every balance entry in ascending {@code (uid, currency)} order.
     *
     * <p>Cold snapshot path only; key extraction and sorting are acceptable here.
     */
    public void forEachSorted(final BalanceConsumer consumer) {
        final int userCount = byUser.size();
        if (userScratch.length < userCount) {
            userScratch = new long[userCount];
        }
        final long[] uids = userScratch;
        final int[] cursor = {0};
        byUser.forEachLong((uid, balances) -> uids[cursor[0]++] = uid);
        Arrays.sort(uids, 0, userCount);

        for (int u = 0; u < userCount; u++) {
            final long uid = uids[u];
            final Long2LongHashMap balances = byUser.get(uid);
            final int currencyCount = balances.size();
            if (currencyScratch.length < currencyCount) {
                currencyScratch = new long[currencyCount];
            }
            final long[] currencies = currencyScratch;
            final int[] c = {0};
            balances.forEachLong((currency, balance) -> currencies[c[0]++] = currency);
            Arrays.sort(currencies, 0, currencyCount);
            for (int i = 0; i < currencyCount; i++) {
                final long currency = currencies[i];
                consumer.accept(uid, (int) currency, balances.get(currency));
            }
        }
    }

    /**
     * Emits every user id in ascending order.
     *
     * <p>Cold snapshot path only; captures account existence even for users
     * that hold no currency balances yet.
     */
    public void forEachUserSorted(final UserConsumer consumer) {
        final int userCount = byUser.size();
        if (userScratch.length < userCount) {
            userScratch = new long[userCount];
        }
        final long[] uids = userScratch;
        final int[] cursor = {0};
        byUser.forEachLong((uid, balances) -> uids[cursor[0]++] = uid);
        Arrays.sort(uids, 0, userCount);
        for (int u = 0; u < userCount; u++) {
            final long uid = uids[u];
            consumer.accept(uid, status(uid));
        }
    }

    /** Primitive callback for deterministic user iteration. */
    @FunctionalInterface
    public interface UserConsumer {
        void accept(long uid, int status);
    }

    /** Primitive callback for deterministic balance iteration. */
    @FunctionalInterface
    public interface BalanceConsumer {
        void accept(long uid, int currency, long balance);
    }
}
