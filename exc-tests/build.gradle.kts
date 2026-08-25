// exc-tests: deterministic replay, snapshot round-trip, idempotency, fault
// injection, and integration tests. Hosts the test-only cluster client harness
// via testFixtures (NOT a shipped Edge SDK).

import org.gradle.testing.jacoco.tasks.JacocoCoverageVerification
import org.gradle.testing.jacoco.tasks.JacocoReport
import org.gradle.testing.jacoco.tasks.JacocoReportBase

plugins {
    `java-test-fixtures`
}

dependencies {
    testImplementation(project(":exc-core"))
    testImplementation(project(":exc-launcher"))
    testImplementation(project(":exc-write-client"))
    testImplementation(project(":exc-read"))
    testImplementation(project(":exc-read-client"))
    testImplementation(project(":exc-gateway"))
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
    // Browser smoke for the bundled gateway UI (opt-in `uiTest` suite).
    testImplementation(libs.playwright)
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

// Multi-node cluster tests. Heavier; wired into `check`.
val clusterTest by tasks.registering(Test::class) {
    description = "Runs multi-node Aeron cluster tests (leader election, catch-up)."
    group = "verification"
    useJUnitPlatform {
        includeTags("cluster")
    }
    shouldRunAfter(integrationTest)
}

// Fault-injection tests (kill-leader mid-ACK). Wired into `check`.
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

// Browser smoke for the bundled gateway UI (headless Chromium via Playwright).
// Opt-in only: it needs browser binaries (`installPlaywrightBrowsers`) and is NOT
// wired into `check`, so the fast, JS-free gate stays unchanged.
val uiTest by tasks.registering(Test::class) {
    description = "Drives the bundled gateway UI in headless Chromium (Playwright)."
    group = "verification"
    useJUnitPlatform {
        includeTags("ui")
    }
    shouldRunAfter(faultTest)
    // Playwright launches a native browser process; give it room under the env.
    systemProperty("exc.ui.headless", "true")
}

// Browser suite that drives the same bundled UI against an EXTERNALLY started
// dev stack (scripts/excoredum-dev.sh). Uses a separate tag (`uiStack`) so the
// self-contained, in-process `uiTest` smoke suite is unaffected. Orchestrated by
// scripts/excoredum-ui-test.sh, which starts/stops the stack around this task.
// Opt-in only (needs a running stack + browser binaries); NOT wired into `check`.
val devStackUiTest by tasks.registering(Test::class) {
    description = "Drives the bundled gateway UI against an externally started dev stack (excoredum-dev.sh)."
    group = "verification"
    useJUnitPlatform {
        includeTags("uiStack")
    }
    shouldRunAfter(faultTest)
    // Playwright launches a native browser process; give it room under the env.
    systemProperty("exc.ui.headless", providers.gradleProperty("exc.ui.headless").getOrElse("true"))
    // Base URL of the gateway started by excoredum-dev.sh (override with
    // -Pexc.gateway.url=http://host:port on the gradle invocation).
    systemProperty("exc.gateway.url", providers.gradleProperty("exc.gateway.url").getOrElse("http://localhost:8080"))
}

// Downloads the Playwright browser binaries used by `uiTest`. Run once before
// `uiTest` on a fresh machine (e.g. `./gradlew :exc-tests:installPlaywrightBrowsers`).
// The smoke suite drives Chromium only, so we install just that browser engine.
val installPlaywrightBrowsers by tasks.registering(JavaExec::class) {
    description = "Downloads Playwright Chromium for the UI smoke suite."
    group = "verification"
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass.set("com.microsoft.playwright.CLI")
    args("install", "chromium")
}

tasks.named<Test>("test") {
    useJUnitPlatform {
        excludeTags("integration", "cluster", "fault", "soak", "ui")
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
// attribute coverage of their main sources to this module's report. All test
// tasks contribute execution data (cluster/fault/soak when they have run), so
// the report reflects the full suite, not just unit + integration.
val coveredProjects = listOf(":exc-core", ":exc-write-client", ":exc-launcher", ":exc-read", ":exc-read-client", ":exc-gateway")
val coverageExecutionData = fileTree(layout.buildDirectory).include(
    "jacoco/test.exec",
    "jacoco/integrationTest.exec",
    "jacoco/clusterTest.exec",
    "jacoco/faultTest.exec",
    "jacoco/soakTest.exec",
)

fun configureCoverageBase(base: JacocoReportBase) {
    coveredProjects.forEach { path ->
        val covered = project(path)
        base.additionalSourceDirs(files(covered.projectDir.resolve("src/main/java")))
        base.additionalClassDirs(fileTree(covered.layout.buildDirectory.dir("classes/java/main")) {
            exclude("**/generated/**")
        })
    }
}

tasks.named<JacocoReport>("jacocoTestReport") {
    dependsOn("test", "integrationTest", "clusterTest", "faultTest")
    executionData(coverageExecutionData)
    configureCoverageBase(this)
}

// Minimum line-coverage floor, run explicitly (not wired into `check`) so the
// threshold can be raised as the suite grows without failing unrelated builds.
tasks.named<JacocoCoverageVerification>("jacocoTestCoverageVerification") {
    description = "Fails when line coverage of the covered production modules drops below the floor."
    group = "verification"
    dependsOn("test", "integrationTest", "clusterTest", "faultTest")
    executionData(coverageExecutionData)
    configureCoverageBase(this)
    violationRules {
        rule {
            limit {
                minimum = "0.60".toBigDecimal()
            }
        }
    }
}
