// exc-gateway: a Netty HTTP/JSON boundary in front of the deterministic CQRS
// matching engine. It is NOT part of the core hot path: it translates UI REST
// calls into read-side queries (ReadClient) and write-side commands (ExcClient).
// JSON stays at this boundary; the engine and its SDKs are untouched.

plugins {
    application
}

application {
    mainClass.set("com.exadbe.gateway.GatewayLauncher")
    // Aeron (embedded media driver) and Netty both touch jdk.internal.misc and
    // sun.nio.ch on modern JDKs; open them for the runtime.
    applicationDefaultJvmArgs = listOf(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    )
}

dependencies {
    // The read/write SDKs are the only door to the engine; they transitively
    // bring aeron-client/driver/agrona and pass the exc-protocol contract on.
    implementation(project(":exc-read-client"))
    implementation(project(":exc-write-client"))
    implementation(project(":exc-protocol"))

    implementation(libs.netty.codec.http)
    implementation(libs.jackson.databind)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.junit.platform.launcher)
}
