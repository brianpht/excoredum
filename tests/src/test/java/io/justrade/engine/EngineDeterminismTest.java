package io.justrade.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.justrade.config.CoreConfig;
import io.justrade.core.CommandOutcome;
import io.justrade.telemetry.CoreMetrics;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/**
 * Property tests: replaying an identical command sequence into two fresh engines
 * yields identical, deterministic state, and signed balance adjustments sum
 * exactly (integer-only, no silent overflow within the bounded range).
 */
class EngineDeterminismTest {

    private static final int USD = 840;

    @Property
    void replayYieldsSumOfDeltasDeterministically(@ForAll("deltaLists") final List<Integer> deltas) {
        long expected = 0L;
        for (final int delta : deltas) {
            expected += delta;
        }

        final long first = run(deltas);
        final long second = run(deltas);

        assertEquals(expected, first);
        assertEquals(first, second);
    }

    private static long run(final List<Integer> deltas) {
        final MatchingEngine engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        final CommandOutcome out = new CommandOutcome();
        final Commands commands = new Commands();

        engine.process(commands.addUser(1L, 0L, 0L, 7L), 0L, out);
        long seq = 1L;
        for (final int delta : deltas) {
            engine.process(commands.adjust(1L, seq, seq, 7L, USD, delta), 0L, out);
            seq++;
        }
        return engine.balance(7L, USD);
    }

    @Provide
    Arbitrary<List<Integer>> deltaLists() {
        // Bounded so a sequence of at most 50 deltas cannot overflow a long.
        return Arbitraries.integers().between(-1_000_000, 1_000_000).list().ofMaxSize(50);
    }
}
