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
    testRuntimeOnly(libs.junit.platform.launcher)
}

// Custom Test tasks must set their classpath explicitly: relying on Gradle's
// Test.classpath convention is deprecated and removed in Gradle 9. They run
// the same test sources as `test`, just filtered by JUnit tag.
fun configureTaggedTest(task: Test) {
    task.testClassesDirs = sourceSets["test"].output.classesDirs
    task.classpath = sourceSets["test"].runtimeClasspath
}

// Integration tests (in-process Media Driver) run under the integrationTest task.
val integrationTest by tasks.registering(Test::class) {
    description = "Runs integration tests with an in-process Aeron Media Driver."
    group = "verification"
    useJUnitPlatform {
        includeTags("integration")
    }
    configureTaggedTest(this)
    shouldRunAfter(tasks.named("test"))
}

// Multi-node cluster tests. Heavier; wired into `check`.
val clusterTest by tasks.registering(Test::class) {
    description = "Runs multi-node Aeron cluster tests (leader election, catch-up)."
    group = "verification"
    useJUnitPlatform {
        includeTags("cluster")
    }
    configureTaggedTest(this)
    shouldRunAfter(integrationTest)
}

// Fault-injection tests (kill-leader mid-ACK). Wired into `check`.
val faultTest by tasks.registering(Test::class) {
    description = "Runs fault-injection tests (leader kill, failover)."
    group = "verification"
    useJUnitPlatform {
        includeTags("fault")
    }
    configureTaggedTest(this)
    shouldRunAfter(clusterTest)
}

// Long-running chaos/soak tests. Opt-in only, NOT wired into `check`.
val soakTest by tasks.registering(Test::class) {
    description = "Runs long-running soak/chaos tests (zero-GC, tail latency)."
    group = "verification"
    useJUnitPlatform {
        includeTags("soak")
    }
    configureTaggedTest(this)
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
