// exc-core: the pure deterministic engine plus the ClusteredService (Aeron
// boundary), dedup, snapshot, and telemetry. The allocation-free hot path.

plugins {
    alias(libs.plugins.jmh)
}

dependencies {
    api(project(":exc-protocol"))
    api(libs.aeron.cluster)
    api(libs.agrona)
    implementation(libs.hdrhistogram)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.jqwik)
    testRuntimeOnly(libs.junit.platform.launcher)
}

// Enforce the determinism / hot-path rule set for core sources.
checkstyle {
    configFile = layout.projectDirectory.file("config/checkstyle/determinism.xml").asFile
}

jmh {
    jmhVersion.set(libs.versions.jmh.get())
    jvmArgs.set(listOf(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    ))
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

// Benchmarks (and JMH-generated sources) are not the production hot path:
// keep lint informative and skip the determinism gate for the jmh source set.
tasks.withType<JavaCompile>().matching { it.name.contains("Jmh") }.configureEach {
    options.compilerArgs.remove("-Werror")
}
tasks.withType<Checkstyle>().matching { it.name.contains("Jmh") }.configureEach {
    enabled = false
}
