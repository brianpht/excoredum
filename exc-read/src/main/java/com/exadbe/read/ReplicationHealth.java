package com.exadbe.read;

/**
 * Replication health published by the read replica. The replica is driven by its
 * caller's thread (see {@link ExcReadReplica#poll()}), which is the sole writer;
 * the volatile fields give cross-thread visibility for an operator reading status.
 */
public final class ReplicationHealth {

    private volatile boolean healthy;
    private volatile long appliedPosition;
    private volatile String activeEndpoint;
    private volatile long failovers;
    private volatile long integrityFailures;
    private volatile long snapshotsLoaded;

    /** Records a successful following cycle at the given cluster-global position. */
    public void markHealthy(final String endpoint, final long position) {
        this.activeEndpoint = endpoint;
        this.appliedPosition = position;
        this.healthy = true;
    }

    /** Records a lost source; the replica keeps serving its last state. */
    public void markStale(final String endpoint, final long position) {
        this.activeEndpoint = endpoint;
        this.appliedPosition = position;
        this.healthy = false;
    }

    /** Counts one source switch. */
    public void recordFailover() {
        failovers++;
    }

    /** Counts one rejected snapshot load (integrity check failed). */
    public void recordIntegrityFailure() {
        integrityFailures++;
    }

    /** Counts one successfully loaded service snapshot. */
    public void recordSnapshotLoaded() {
        snapshotsLoaded++;
    }

    public boolean isHealthy() {
        return healthy;
    }

    public long appliedPosition() {
        return appliedPosition;
    }

    /** The archive control channel currently followed, or the last attempted one. */
    public String activeEndpoint() {
        return activeEndpoint;
    }

    /** Number of failover events since this replica started. */
    public long failovers() {
        return failovers;
    }

    /** Number of snapshot loads rejected by the integrity check. */
    public long integrityFailures() {
        return integrityFailures;
    }

    /** Number of service snapshots loaded into the engine. */
    public long snapshotsLoaded() {
        return snapshotsLoaded;
    }
}
