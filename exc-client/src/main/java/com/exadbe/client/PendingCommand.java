package com.exadbe.client;

import org.agrona.concurrent.UnsafeBuffer;

/**
 * A pooled, reusable holder for an in-flight command: its pre-encoded bytes (for
 * idempotent resend), identity, and retry bookkeeping. Instances are recycled by
 * {@link ExcClient} so steady-state submission is allocation-free.
 */
final class PendingCommand {

    /** Encoded command bytes (header + envelope), resent verbatim on retry. */
    final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);

    final int poolIndex;

    int length;
    long commandIdHi;
    long commandIdLo;
    long submitNanos;
    long deadlineNanos;
    int retries;
    boolean inUse;

    PendingCommand(final int poolIndex) {
        this.poolIndex = poolIndex;
    }

    void reset() {
        length = 0;
        commandIdHi = 0L;
        commandIdLo = 0L;
        submitNanos = 0L;
        deadlineNanos = 0L;
        retries = 0;
        inUse = false;
    }
}
