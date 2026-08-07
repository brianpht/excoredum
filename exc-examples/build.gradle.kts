// exc-examples: runnable, end-to-end demonstrations of the engine. Depends on the
// launcher (to boot an in-process single-node cluster) and the client SDK (to
// submit commands and read results). Not part of the deterministic hot path.

plugins {
    application
}

dependencies {
    implementation(project(":exc-launcher"))
    implementation(project(":exc-client"))
    implementation(project(":exc-core"))
    implementation(project(":exc-protocol"))
    implementation(libs.bundles.aeron)
}

application {
    mainClass.set("com.exadbe.examples.QuickStartExample")
    applicationDefaultJvmArgs = listOf(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    )
}

// Example code is illustrative, not the production hot path.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.remove("-Werror")
}
