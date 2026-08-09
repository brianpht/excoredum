// exc-xcore-bench: comparative benchmarks against the upstream exchange-core
// (exchange.core2:exchange-core). Four layers: JMH matching-level order-book
// comparison, single-thread engine dispatch latency vs exchange-core's disruptor
// pipeline, closed-loop end-to-end latency, and replay throughput. Benchmark and
// glue code only - exempt from the core determinism rules.

plugins {
    application
    alias(libs.plugins.jmh)
}

// Aeron/Agrona opens plus the set chronicle (pulled by exchange-core) needs
// for its reflective fast paths on JDK 21.
val benchJvmArgs = listOf(
    "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
    "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-exports", "java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens", "java.base/java.lang=ALL-UNNAMED",
    "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens", "java.base/java.io=ALL-UNNAMED",
    "--add-opens", "java.base/java.nio=ALL-UNNAMED",
    "--add-opens", "java.base/java.util=ALL-UNNAMED",
)

application {
    mainClass.set("com.exadbe.xcorebench.XcoreBenchMain")
    applicationDefaultJvmArgs = benchJvmArgs
}

dependencies {
    implementation(project(":exc-core"))
    implementation(project(":exc-launcher"))
    implementation(project(":exc-client"))
    implementation(project(":exc-protocol"))
    implementation(project(":exc-bench"))
    implementation(libs.exchange.core2)
    // Upgrade exchange-core's chronicle-wire to a JDK 21-compatible line;
    // 2.19.1 fails reflective lookup of sun.nio.ch.FileChannelImpl.unmap0.
    implementation(libs.chronicle.wire)
    implementation(libs.bundles.aeron)
    implementation(libs.hdrhistogram)

    // exchange-core logs via slf4j; give it a binding so startup noise is visible.
    runtimeOnly(libs.slf4j.simple)
}

jmh {
    jmhVersion.set(libs.versions.jmh.get())
    jvmArgs.set(benchJvmArgs)
    resultFormat.set("JSON")
    resultsFile.set(layout.buildDirectory.file("results/jmh/results.json"))
    // -Pjmh.profilers=gc attaches JMH profilers (e.g. the GC allocation profiler).
    if (project.hasProperty("jmh.profilers")) {
        profilers.set((project.property("jmh.profilers") as String)
            .split(",").map { it.trim() }.filter { it.isNotEmpty() })
    }
    // -PquickBench: fast smoke run used by the CI gate.
    if (project.hasProperty("quickBench")) {
        warmupIterations.set(1)
        iterations.set(1)
        fork.set(1)
    }
}

// Benchmark code is illustrative, not the production hot path: keep lint
// informative but non-fatal so the harness stays readable.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.remove("-Werror")
}
tasks.withType<JavaCompile>().matching { it.name.contains("Jmh") }.configureEach {
    options.compilerArgs.remove("-Werror")
}
tasks.withType<Checkstyle>().matching { it.name.contains("Jmh") }.configureEach {
    enabled = false
}
