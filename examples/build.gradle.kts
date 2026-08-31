// examples: runnable, end-to-end demonstrations of the engine. Depends on the
// launcher (to boot an in-process single-node cluster) and the client SDK (to
// submit commands and read results). Not part of the deterministic hot path.

plugins {
    application
}

dependencies {
    implementation(project(":launcher"))
    implementation(project(":write-client"))
    implementation(project(":core"))
    implementation(project(":protocol"))
    implementation(libs.bundles.aeron)
}

application {
    mainClass.set("io.justrade.examples.QuickStartExample")
    applicationDefaultJvmArgs = listOf(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    )
}

// Example code is illustrative, not the production hot path.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.remove("-Werror")
}
