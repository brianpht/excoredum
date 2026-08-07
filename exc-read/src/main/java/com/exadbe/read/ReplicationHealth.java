package com.exadbe.read;

/**
 * Replication health published by the read replica. The replica is driven by its
 * caller's thread (see {@link ExcReadReplica#poll()}), which is the sole writer;
 * the volatile fields give cross-thread visibility for an operator reading status.
 */
public final class ReplicationHealth {

    private volatile boolean healthy;
    private volatile long appliedPosition;

    /** Records a successful following cycle at the given cluster-global position. */
    public void markHealthy(final long position) {
        this.appliedPosition = position;
        this.healthy = true;
    }

    /** Records a lost source; the replica keeps serving its last state. */
    public void markStale() {
        this.healthy = false;
    }

    public boolean isHealthy() {
        return healthy;
    }

    public long appliedPosition() {
        return appliedPosition;
    }
}
