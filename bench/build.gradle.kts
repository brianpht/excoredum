// bench: end-to-end load drivers that boot an in-process cluster and drive it
// with the client, reporting throughput and tail latency. JMH micro-benchmarks
// for the hot path live in core's jmh source set.

plugins {
    application
}

application {
    mainClass.set("io.justrade.bench.ExcBenchHarness")
    applicationDefaultJvmArgs = listOf(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    )
}

dependencies {
    implementation(project(":launcher"))
    implementation(project(":write-client"))
    implementation(project(":read-client"))
    implementation(project(":core"))
    implementation(project(":protocol"))
    implementation(libs.bundles.aeron)
    implementation(libs.hdrhistogram)
    implementation(libs.jackson.databind)
}

// Benchmark code is illustrative, not the production hot path: keep lint
// informative but non-fatal so the harness stays readable.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.remove("-Werror")
}
