// exc-tests: deterministic replay, snapshot round-trip, idempotency, fault
// injection, and integration tests. Hosts the test-only cluster client harness
// via testFixtures (NOT a shipped Edge SDK).

import org.gradle.testing.jacoco.tasks.JacocoReport

plugins {
    `java-test-fixtures`
}

dependencies {
    testImplementation(project(":exc-core"))
    testImplementation(project(":exc-launcher"))
    testImplementation(project(":exc-write-client"))
    testImplementation(project(":exc-read"))
    testImplementation(project(":exc-read-client"))
    testImplementation(project(":exc-bench"))
    testImplementation(project(":exc-xcore-bench"))
    testImplementation(libs.bundles.aeron)

    testFixturesApi(project(":exc-protocol"))
    testFixturesApi(project(":exc-core"))
    testFixturesApi(project(":exc-launcher"))
    testFixturesApi(libs.bundles.aeron)

    testImplementation(libs.hdrhistogram)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.jqwik)
    testRuntimeOnly(libs.junit.platform.launcher)
}

// Integration tests (in-process Media Driver) run under the integrationTest task.
val integrationTest by tasks.registering(Test::class) {
    description = "Runs integration tests with an in-process Aeron Media Driver."
    group = "verification"
    useJUnitPlatform {
        includeTags("integration")
    }
    shouldRunAfter(tasks.named("test"))
}

// Multi-node cluster tests. Heavier; opt-in, NOT wired into `check`.
val clusterTest by tasks.registering(Test::class) {
    description = "Runs multi-node Aeron cluster tests (leader election, catch-up)."
    group = "verification"
    useJUnitPlatform {
        includeTags("cluster")
    }
    shouldRunAfter(integrationTest)
}

// Fault-injection tests (kill-leader mid-ACK). Opt-in, NOT wired into `check`.
val faultTest by tasks.registering(Test::class) {
    description = "Runs fault-injection tests (leader kill, failover)."
    group = "verification"
    useJUnitPlatform {
        includeTags("fault")
    }
    shouldRunAfter(clusterTest)
}

// Long-running chaos/soak tests. Opt-in only, NOT wired into `check`.
val soakTest by tasks.registering(Test::class) {
    description = "Runs long-running soak/chaos tests (zero-GC, tail latency)."
    group = "verification"
    useJUnitPlatform {
        includeTags("soak")
    }
    shouldRunAfter(faultTest)
}

tasks.named<Test>("test") {
    useJUnitPlatform {
        excludeTags("integration", "cluster", "fault", "soak")
    }
}

tasks.named("check") {
    dependsOn(integrationTest, clusterTest, faultTest)
}

// The exchange-core comparison smoke test boots exchange-core's disruptor,
// whose chronicle stack needs these reflective opens on JDK 21.
tasks.withType<Test>().configureEach {
    jvmArgs(
        "--add-exports", "java.base/jdk.internal.ref=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens", "java.base/java.io=ALL-UNNAMED",
        "--add-opens", "java.base/java.nio=ALL-UNNAMED",
        "--add-opens", "java.base/java.util=ALL-UNNAMED",
    )
    // SystemLoadIntegrationTest scale knob: -Dexc.systemload.ops / -Dexc.systemload.users
    // lets the docker-equivalent run be reproduced at full scale in one JVM.
    systemProperty("exc.systemload.ops", System.getProperty("exc.systemload.ops", "20000"))
    systemProperty("exc.systemload.users", System.getProperty("exc.systemload.users", "20"))
    // ChaosSoakTest scale knob: -Dexc.soak.warmupRounds / -Dexc.soak.steadyRounds
    // (one round is one step of an 8-step workload pattern; 8 rounds = 13 commands).
    systemProperty("exc.soak.warmupRounds", System.getProperty("exc.soak.warmupRounds", "15000"))
    systemProperty("exc.soak.steadyRounds", System.getProperty("exc.soak.steadyRounds", "120000"))
}

// Test code is not the production hot path; keep lint informative but non-fatal.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.remove("-Werror")
}

// Coverage: the suite lives here but exercises the production modules, so
// attribute coverage of their main sources to this module's report.
tasks.named<JacocoReport>("jacocoTestReport") {
    val coveredProjects = listOf(":exc-core", ":exc-write-client", ":exc-launcher", ":exc-read", ":exc-read-client")
    dependsOn("test", "integrationTest")
    executionData(fileTree(layout.buildDirectory).include("jacoco/test.exec", "jacoco/integrationTest.exec"))
    coveredProjects.forEach { path ->
        val covered = project(path)
        additionalSourceDirs(files(covered.projectDir.resolve("src/main/java")))
        additionalClassDirs(fileTree(covered.layout.buildDirectory.dir("classes/java/main")) {
            exclude("**/generated/**")
        })
    }
}
