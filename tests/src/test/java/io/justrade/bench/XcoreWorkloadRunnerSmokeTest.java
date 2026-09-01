package io.justrade.bench;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.config.CoreConfig;
import io.justrade.launcher.ClusterConfig;
import io.justrade.launcher.ClusterNode;
import io.justrade.xcorebench.Workload;
import io.justrade.xcorebench.WorkloadGenerator;
import io.justrade.xcorebench.XcoreWorkloadRunner;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Smoke test for the deployed exchange-core workload runner: replays a small
 * {@code TestOrdersGenerator} workload through a single-node in-process cluster
 * with generous funding and asserts the full-risk engine reproduces the
 * matching-only reference outcome (every command succeeds and fills match
 * {@code JustradeBookRunner}). This validates the runner's command mapping and
 * funding sizing before the AWS run.
 */
@Tag("integration")
class XcoreWorkloadRunnerSmokeTest {

    @Test
    @Timeout(180)
    void throughputReplayMatchesReference() throws Exception {
        final Path baseDir = Files.createTempDirectory("xcore-workload-smoke-");
        final Workload workload =
                WorkloadGenerator.generate(2_000, 256, 100, XcoreWorkloadRunner.SYMBOL, false, false, 1);
        final boolean ok;
        try (ClusterNode ignored =
                new ClusterNode(ClusterConfig.singleNodeLocalhost(0, baseDir), CoreConfig.defaults())) {
            ok = XcoreWorkloadRunner.run(
                    "throughput",
                    workload,
                    100,
                    ClusterConfig.ingressEndpoints(1),
                    "aeron:udp?endpoint=localhost:0",
                    1L,
                    16,
                    2_000L,
                    0);
        }
        assertTrue(ok, "the full-risk replay must reproduce the reference outcome");
    }
}
