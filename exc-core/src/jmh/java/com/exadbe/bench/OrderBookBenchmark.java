package com.exadbe.bench;

import com.exadbe.core.CommandOutcome;
import com.exadbe.engine.orderbook.OrderBookNaive;
import java.util.concurrent.TimeUnit;
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
 * Order-book micro-benchmarks driving {@link OrderBookNaive} directly (no risk,
 * no cluster). {@code restingPlaceCancel} exercises pooled insert/remove;
 * {@code iocMatch} exercises the price-time matching loop against deep resting
 * liquidity. Run with {@code -prof gc} to assert zero steady-state allocation.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class OrderBookBenchmark {

    private static final int SYMBOL = 1;
    private static final long TAKER_UID = 2L;

    private OrderBookNaive placeCancelBook;
    private OrderBookNaive matchBook;
    private CommandOutcome outcome;
    private long orderId;

    @Setup(Level.Trial)
    public void setup() {
        outcome = new CommandOutcome(1024);

        placeCancelBook = new OrderBookNaive(SYMBOL);

        // A single deep resting ask that IOC bids nibble at without depleting.
        matchBook = new OrderBookNaive(SYMBOL);
        outcome.reset(0L, 0L);
        matchBook.placeGtc(1L, true, 100L, Long.MAX_VALUE / 4L, 0L, 99L, 0L, outcome);
    }

    @Benchmark
    public long restingPlaceCancel() {
        final long id = ++orderId;
        outcome.reset(0L, id);
        // A far-from-touch bid that never crosses; rests then is cancelled.
        placeCancelBook.placeGtc(id, false, 50L, 10L, 50L, 1L, 0L, outcome);
        placeCancelBook.cancel(id, 1L, outcome);
        return id;
    }

    @Benchmark
    public long iocMatch() {
        final long id = ++orderId;
        outcome.reset(0L, id);
        return matchBook.matchIoc(id, false, 100L, 1L, 100L, TAKER_UID, outcome);
    }
}
