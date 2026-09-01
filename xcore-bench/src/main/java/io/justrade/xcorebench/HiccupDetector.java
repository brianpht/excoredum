package io.justrade.xcorebench;

/**
 * Samples the system monotonic clock in a tight loop and records the largest gap
 * between consecutive samples - the worst scheduling pause the running JVM
 * observed. exchange-core's {@code PerfHiccups} measures the same signal for its
 * own process; here the detector runs on the load generator while the cluster is
 * under load, so it captures client-side pauses plus any pause the client feels
 * indirectly as delayed acknowledgements.
 */
public final class HiccupDetector implements AutoCloseable {

    private final Thread thread;
    private volatile boolean running;
    private long maxGapNanos;
    private long samples;

    public HiccupDetector() {
        this.thread = new Thread(this::run, "hiccup-detector");
        this.thread.setDaemon(true);
    }

    /** Starts sampling; resets the captured maximum and sample count. */
    public void start() {
        maxGapNanos = 0L;
        samples = 0L;
        running = true;
        thread.start();
    }

    private void run() {
        long prev = System.nanoTime();
        while (running) {
            final long now = System.nanoTime();
            final long gap = now - prev;
            if (gap > maxGapNanos) {
                maxGapNanos = gap;
            }
            samples++;
            prev = now;
        }
    }

    /** Stops the sampler and joins it so its results are visible to the caller. */
    @Override
    public void close() {
        running = false;
        try {
            thread.join(1_000L);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Largest gap between consecutive samples, in nanoseconds (0 when never started). */
    public long maxGapNanos() {
        return maxGapNanos;
    }

    /** Number of samples taken since {@link #start()}. */
    public long samples() {
        return samples;
    }
}
