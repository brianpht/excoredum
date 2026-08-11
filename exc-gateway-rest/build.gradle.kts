// exc-gateway-rest: the REST API gateway. Bridges HTTP/JSON to the cluster -
// writes go through the exc-client SDK (idempotent, leader-aware), reads are
// served from an embedded exc-read replica (eventually consistent, CQRS). The
// gateway is a boundary service: JSON and string handling live here and never
// cross into the deterministic core.

plugins {
    application
}

application {
    mainClass.set("com.exadbe.gateway.RestGatewayLauncher")
    applicationDefaultJvmArgs = listOf(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    )
}

dependencies {
    implementation(project(":exc-client"))
    implementation(project(":exc-read"))
    implementation(project(":exc-core"))
    implementation(libs.netty.codec.http)
}
