// write-client: the write-side client SDK. It consumes ONLY the protocol wire
// contract (CommandEnvelope / CommandResult) and deliberately does NOT depend on
// core. It adds leader-change handling, idempotent retry, async
// request/response correlation, and backpressure signalling on an Aeron cluster
// client.

dependencies {
    api(project(":protocol"))
    api(libs.aeron.cluster)
    api(libs.agrona)
    api(libs.hdrhistogram)
    implementation(libs.aeron.driver)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}
