package com.exadbe.telemetry;

/**
 * Single-writer core counters. All mutation happens on the one service thread,
 * so plain long fields are sufficient and lock-free.
 *
 * <p>Phase 5 moves these to off-heap Agrona {@code AtomicCounter}s so operators
 * can read them from another thread without perturbing the hot path.
 */
public final class CoreMetrics {

    private long commandsProcessed;
    private long duplicates;
    private long backpressureEvents;
    private long unsupportedCommands;

    public void onCommandProcessed() {
        commandsProcessed++;
    }

    public void onDuplicate() {
        duplicates++;
    }

    public void onBackpressure() {
        backpressureEvents++;
    }

    public void onUnsupportedCommand() {
        unsupportedCommands++;
    }

    public long commandsProcessed() {
        return commandsProcessed;
    }

    public long duplicates() {
        return duplicates;
    }

    public long backpressureEvents() {
        return backpressureEvents;
    }

    public long unsupportedCommands() {
        return unsupportedCommands;
    }
}
