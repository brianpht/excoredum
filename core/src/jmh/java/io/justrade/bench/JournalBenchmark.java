package io.justrade.bench;

import io.justrade.core.CommandOutcome;
import io.justrade.journal.DomainEventJournal;
import io.justrade.journal.EventJournalRing;
import io.justrade.telemetry.CoreMetrics;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Domain-event journal emission cost: encode a command's matcher events into the
 * off-heap ring and drain them. Run with {@code -prof gc} to assert zero
 * steady-state allocation on the producer path.
 */
@State(Scope.Thread)
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class JournalBenchmark {

    private static final int SYM = 1;

    private EventJournalRing ring;
    private DomainEventJournal journal;
    private CommandOutcome outcome;
    private EventJournalRing.EventHandler drain;
    private IdleStrategy idle;
    private long logPosition;

    @Setup(Level.Trial)
    public void setup() {
        ring = new EventJournalRing(1 << 12, 128);
        journal = new DomainEventJournal(ring, new CoreMetrics());
        drain = (buffer, offset, length) -> {};
        idle = new NoOpIdleStrategy();
        outcome = new CommandOutcome(64);
        outcome.reset(0L, 1L);
        // A representative command that swept four resting makers.
        for (int i = 0; i < 4; i++) {
            outcome.addTrade(SYM, 100L + i, 11L, 22L, 500L, 3L, true, false, 0L, true, 600L);
        }
    }

    @Benchmark
    public void emitAndDrain() {
        journal.emit(outcome, logPosition++, 1_234L, idle);
        ring.poll(drain, 8);
    }
}
