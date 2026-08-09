package com.exadbe.bench;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.xcorebench.BookComparison;
import com.exadbe.xcorebench.EngineComparison;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Smoke tests for the exchange-core comparison module: the book-level replay
 * must cross-validate (excoredum's book vs both exchange-core implementations
 * produce identical event counters and full-depth L2), and the engine-path
 * comparison must boot exchange-core's disruptor pipeline on this JDK.
 */
@Tag("integration")
class XcoreBenchSmokeTest {

    @Test
    @Timeout(180)
    void bookReplayCrossValidates() {
        final String report =
                BookComparison.run(new BookComparison.BookComparisonConfig(2_000, 256, 100, 1, 1, false, false));
        assertTrue(report.contains("cross-validation: PASS"), "cross-validation must pass:\n" + report);
    }

    @Test
    @Timeout(180)
    void engineComparisonRuns() {
        final String report = EngineComparison.run(100, 300);
        assertTrue(report.contains("exc-core engine dispatch"), "exc row present:\n" + report);
        assertTrue(report.contains("xcore disruptor pipeline"), "xcore row present:\n" + report);
    }
}
